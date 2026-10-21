# -*- coding: utf-8 -*-
"""🙋 RESPONSABLE DE LA OT (F0, REGLA #36) — Daniel, 20-oct-2026:
«¿Los avisos le llegan a la persona que administra las órdenes de trabajo (Víctor, Aarón o Juan Pablo)? Quiero que ese rol de
responsable de la OT quede bien establecido.»

Decisiones de Daniel que se vigilan:
  1. El responsable es un PERMISO («Responsable de OT» en Roles), apagado para todos y encendido solo para esas tres personas. Mientras
     nadie lo tenga, TODO está dormido: no se pide nada, no se bloquea nada y los avisos siguen yendo a creador + ejecutivo + respaldo.
  2. Al crear: quien la crea si tiene el permiso; si no, el ejecutivo del cliente; si tampoco hay, el asistente PIDE elegirlo (REGLA #32).
     Todos los caminos que crean OT pasan por UNA función (`_ot_responsable_al_crear`).
  3. Cambiarlo: solo gestión (superadmin/admin/supervisor) y el propio responsable; motivo opcional; queda en la bitácora (quién, cuándo,
     de quién a quién) y se avisa con tono amable a la persona nueva y a la anterior (campana, nunca correo ni cliente: REGLA #22).
  4. Se ve en la ficha (chip; un técnico lo ve SOLO como nombre) y en la tabla de OT (columna + filtro «Mis OT como responsable»).
  5. Las OT anteriores NO se rellenan: con la columna en NULL todo cae al respaldo de siempre.

Se prueba (sin BD, sin Flask, sin correos reales):
  A. el módulo puro ot_responsable.py;
  B. las funciones de app.py con una base de mentira (candidatos, defecto, escritura al crear, cambio, ficha, panel);
  C. el cableado (permiso, ENUM de la campana, arranque barato REGLA #18, ganchos en cada camino que crea OT, avisos de aprobación);
  D. las plantillas RENDERIZADAS de verdad (tabla del panel, chip de la ficha) y el JS del asistente y de la ficha ejecutado con Node.
Correr:  python -m pytest tests/test_ot_responsable.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest

import jinja2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_responsable as R  # noqa: E402
from tests._fuente_app import RAIZ, decoradores, fuente_constante, texto  # noqa: E402
from tests._fuente_app import fuente_funcion as _fuente_funcion  # noqa: E402

HAY_NODE = shutil.which("node") is not None
APP = texto("app.py")
_PLATA = re.compile(r"\$\s?\d|\bproveedor|costo|precio|margen|factura|saldo|valorizad|\bOC\b|monto|cobr|ganancia|pérdida", re.I)


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


_COMPILADO = {}


def cargar(funciones=(), constantes=(), extra=None):
    """Igual que tests._fuente_app.cargar, pero compila cada función/constante UNA sola vez por corrida (buscar en los 9 MB de app.py
    cuesta ~0.15 s por función): cada prueba recibe un ámbito nuevo con sus propios valores mutables."""
    import ast
    amb = dict(extra or {})
    for c in constantes:
        if ("c", c) not in _COMPILADO:
            _COMPILADO[("c", c)] = compile(ast.parse(fuente_constante(c)), "<app:%s>" % c, "exec")
        exec(_COMPILADO[("c", c)], amb)
    for f in funciones:
        if ("f", f) not in _COMPILADO:
            arbol = ast.parse(fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _COMPILADO[("f", f)] = compile(arbol, "<app:%s>" % f, "exec")
        exec(_COMPILADO[("f", f)], amb)
    return amb


_FUENTES = {}


def fuente_funcion(nombre, *a, **k):          # noqa: F811  (con memoria: la misma función se pide muchas veces)
    if (nombre, a, tuple(sorted(k.items()))) not in _FUENTES:
        _FUENTES[(nombre, a, tuple(sorted(k.items())))] = _fuente_funcion(nombre, *a, **k)
    return _FUENTES[(nombre, a, tuple(sorted(k.items())))]


def _codigo(nombre):
    """El código de una función de app.py SIN su docstring ni sus comentarios (para vigilar lo que NO debe hacer sin que lo frene su explicación)."""
    import ast
    arbol = ast.parse(fuente_funcion(nombre))
    for n in ast.walk(arbol):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.body and isinstance(n.body[0], ast.Expr) \
                and isinstance(getattr(n.body[0], "value", None), ast.Constant) and isinstance(n.body[0].value.value, str):
            n.body = n.body[1:] or [ast.Pass()]
    return ast.unparse(arbol)


# Las personas de mentira (lo que devolvería app_users + el permiso EXPLÍCITO por persona `ov` y por rol `rolp`).
USUARIOS = [
    {"id": 1, "username": "daniel.aguilar@sphs.cl", "nombre": "Daniel Aguilar", "role": "superadmin", "ov": None, "rolp": None},
    {"id": 2, "username": "victor@sphs.cl", "nombre": "Víctor Soto", "role": "ejecutivo", "ov": 1, "rolp": None},          # por persona
    {"id": 3, "username": "aaron@sphs.cl", "nombre": "Aarón Rojas", "role": "ejecutivo_sstt", "ov": None, "rolp": 1},      # por rol
    {"id": 4, "username": "juanpablo@sphs.cl", "nombre": "Juan Pablo Díaz", "role": "supervisor", "ov": 1, "rolp": None},
    {"id": 5, "username": "isabel", "nombre": "Isabel Milling", "role": "tecnico_externo", "ov": 1, "rolp": 1},             # técnica: NUNCA
    {"id": 7, "username": "admin@sphs.cl", "nombre": "Administración", "role": "admin", "ov": None, "rolp": None},          # sin permiso
    {"id": 8, "username": "otro@sphs.cl", "nombre": "Otro Ejecutivo", "role": "ejecutivo", "ov": 0, "rolp": 1},              # la persona lo apagó
    {"id": 9, "username": "jaizer@sphs.cl", "nombre": "Jaizer Araujo", "role": "tecnico_ejecutivo", "ov": None, "rolp": 1},   # técnico elevado: NUNCA
]


# ═══════════════════════════════════ A) el módulo puro ═══════════════════════════════════
class TestEntero(unittest.TestCase):
    def test_solo_enteros_positivos(self):
        for bueno, esperado in (("7", 7), (7, 7), (7.0, 7), (" 12 ", 12), ("3.0", 3)):
            self.assertEqual(R.entero(bueno), esperado, bueno)
        for malo in (None, "", 0, "0", -4, "x", True, False, "1e999", float("nan"), [], {}):
            self.assertIsNone(R.entero(malo), repr(malo))


class TestQuienPuedeSerResponsable(unittest.TestCase):
    def test_la_persona_manda_sobre_el_rol_y_sin_filas_nadie_lo_es(self):
        self.assertTrue(R.tiene_permiso(1, 0))        # encendido para ella aunque el rol no lo tenga
        self.assertFalse(R.tiene_permiso(0, 1))       # apagado para ella aunque el rol sí lo tenga
        self.assertTrue(R.tiene_permiso(None, 1))     # hereda del rol
        self.assertFalse(R.tiene_permiso(None, 0))
        self.assertFalse(R.tiene_permiso(None, None))  # nace APAGADO para todos

    def test_activo_con_permiso_y_fuera_de_la_familia_tecnico(self):
        self.assertTrue(R.es_candidato("ejecutivo", True, 1, None))
        self.assertFalse(R.es_candidato("ejecutivo", False, 1, 1), "un usuario inactivo no puede ser responsable")
        self.assertFalse(R.es_candidato("tecnico", True, 1, 1), "un técnico nunca (REGLA #19 y #26)")
        self.assertFalse(R.es_candidato("superadmin", True, None, None), "el superadmin NO es responsable por tener todas las llaves")

    def test_lista_de_candidatos_de_las_filas_de_mentira(self):
        fam = cargar(funciones=("_rol_familia",))["_rol_familia"]
        c = R.candidatos_de_filas(USUARIOS, fam)
        self.assertEqual([x["id"] for x in c], [3, 4, 2])                      # Aarón, Juan Pablo, Víctor (por nombre)
        self.assertEqual([x["nombre"] for x in c], ["Aarón Rojas", "Juan Pablo Díaz", "Víctor Soto"])
        self.assertTrue({1, 5, 7, 8, 9}.isdisjoint({x["id"] for x in c}))      # superadmin, técnicos, sin permiso, apagado a mano
        repetidas = R.candidatos_de_filas(USUARIOS + [dict(USUARIOS[1])], fam)
        self.assertEqual(len(repetidas), 3, "sin repetidos")
        self.assertEqual(R.candidatos_de_filas(None, fam), [])

    def test_nombre_cae_al_usuario_y_se_limpia(self):
        fam = lambda r: "ejecutivo"
        c = R.candidatos_de_filas([{"id": 2, "username": "v@sphs.cl", "nombre": None, "role": "ejecutivo", "ov": 1, "rolp": None},
                                   {"id": 3, "username": "x", "nombre": "  Aarón \n  Rojas  ", "role": "ejecutivo", "ov": 1, "rolp": None}], fam)
        self.assertEqual({x["id"]: x["nombre"] for x in c}, {2: "v@sphs.cl", 3: "Aarón Rojas"})


class TestElDefectoAlCrear(unittest.TestCase):
    CANDS = [2, 3, 4]

    def test_quien_la_crea_si_tiene_el_permiso(self):
        self.assertEqual(R.decidir_defecto(self.CANDS, 4, 3), (4, R.ORIGEN_CREADOR))     # gana sobre el ejecutivo del cliente
        self.assertEqual(R.decidir_defecto(self.CANDS, "2", None), (2, R.ORIGEN_CREADOR))

    def test_si_no_el_ejecutivo_del_cliente_si_tambien_lo_tiene(self):
        self.assertEqual(R.decidir_defecto(self.CANDS, 7, 3), (3, R.ORIGEN_EJECUTIVO))   # el creador (admin) no tiene el permiso
        self.assertEqual(R.decidir_defecto(self.CANDS, None, 3), (3, R.ORIGEN_EJECUTIVO))

    def test_si_tampoco_hay_nadie_y_el_asistente_pide_elegirlo(self):
        self.assertEqual(R.decidir_defecto(self.CANDS, 7, None), (None, None))
        self.assertEqual(R.decidir_defecto(self.CANDS, 7, 8), (None, None), "el ejecutivo del cliente sin el permiso no es responsable")
        self.assertEqual(R.decidir_defecto([], 4, 3), (None, None), "sin candidatos todo está dormido")
        self.assertEqual(R.decidir_defecto(None, 4, 3), (None, None))

    def test_lo_elegido_se_revalida(self):
        self.assertEqual(R.validar_elegido("3", self.CANDS), 3)
        self.assertIsNone(R.validar_elegido(7, self.CANDS))
        self.assertIsNone(R.validar_elegido("basura", self.CANDS))
        self.assertIsNone(R.validar_elegido(None, self.CANDS))

    def test_el_origen_se_explica_para_la_bitacora(self):
        self.assertIn("quien creó", R.texto_origen(R.ORIGEN_CREADOR))
        self.assertIn("ejecutivo del cliente", R.texto_origen(R.ORIGEN_EJECUTIVO))
        self.assertIn("OT-2026-00100", R.texto_origen(R.ORIGEN_HEREDADO, madre="OT-2026-00100"))
        self.assertIn("Víctor", R.texto_origen(R.ORIGEN_ELEGIDO, quien="Víctor"))
        self.assertEqual(R.texto_origen("otra cosa"), "")


class TestCambiarElResponsable(unittest.TestCase):
    def test_solo_gestion_y_el_propio_responsable(self):
        for fam in ("superadmin", "admin", "supervisor"):
            self.assertTrue(R.puede_cambiar(fam, 99, None), fam)
            self.assertTrue(R.puede_cambiar(fam, 99, 3), fam)
        self.assertTrue(R.puede_cambiar("ejecutivo", 3, 3), "el propio responsable (aunque sea de la familia ejecutivo)")
        self.assertFalse(R.puede_cambiar("ejecutivo", 2, 3), "otro ejecutivo no reasigna lo de un colega")
        self.assertFalse(R.puede_cambiar("ejecutivo", 3, None), "una OT sin responsable la asigna la gestión")
        self.assertFalse(R.puede_cambiar("tecnico", 3, 3), "un técnico nunca, ni siquiera si figurara como responsable")
        self.assertFalse(R.puede_cambiar("", 3, 3 - 1))

    def test_validar_el_cambio(self):
        cands = [2, 3, 4]
        self.assertEqual(R.validar_cambio(3, 4, cands, "en_curso", False), (True, None, ""))
        self.assertEqual(R.validar_cambio(None, 4, cands, "programada", False)[0], True)
        ok, cod, msg = R.validar_cambio(3, None, cands, "programada", False)
        self.assertEqual((ok, cod), (False, "RESPONSABLE_REQUERIDO"))
        ok, cod, msg = R.validar_cambio(3, 7, cands, "programada", False)
        self.assertEqual((ok, cod), (False, "RESPONSABLE_NO_VALIDO"))
        self.assertIn("Responsable de OT", msg)
        ok, cod, msg = R.validar_cambio(3, 3, cands, "programada", False)
        self.assertEqual((ok, cod), (False, "YA_ES_RESPONSABLE"))

    def test_una_ot_terminada_solo_la_mueve_el_superadmin(self):
        for est in ("cerrada", "cancelada", "anulada", "CERRADA"):
            self.assertEqual(R.validar_cambio(3, 4, [3, 4], est, False)[1], "OT_TERMINADA", est)
            self.assertTrue(R.validar_cambio(3, 4, [3, 4], est, True)[0], est)
        for est in ("programada", "en_curso", "pendiente_aprobacion", "completada", "firmada_tecnico", None):
            self.assertTrue(R.validar_cambio(3, 4, [3, 4], est, False)[0], est)

    def test_el_motivo_es_opcional_y_una_sola_linea(self):
        self.assertEqual(R.normalizar_motivo(None), "")
        self.assertEqual(R.normalizar_motivo("  Aarón\nestá   de vacaciones  "), "Aarón está de vacaciones")
        self.assertEqual(len(R.normalizar_motivo("x" * 900)), R.MOTIVO_MAX)


class TestTextosAmables(unittest.TestCase):
    """Daniel: «aviso suave… nunca algo que suene a castigo»."""

    def _todos(self):
        return [
            R.titulo_aviso_nuevo("OT-2026-00123"),
            R.cuerpo_aviso_nuevo("OT-2026-00123", "Gimnasio X", "Daniel Aguilar", R.ORIGEN_CAMBIO, "Aarón está de vacaciones", "Aarón Rojas"),
            R.cuerpo_aviso_nuevo("OT-2026-00123", "Gimnasio X", "Daniel Aguilar", R.ORIGEN_EJECUTIVO),
            R.cuerpo_aviso_nuevo("OT-2026-00123", "Gimnasio X", "Víctor Soto", R.ORIGEN_ELEGIDO),
            R.titulo_aviso_anterior("OT-2026-00123", "Víctor Soto"),
            R.cuerpo_aviso_anterior("OT-2026-00123", "Gimnasio X", "Daniel Aguilar", "Víctor Soto", "Aarón está de vacaciones"),
            R.cuerpo_aviso_anterior("OT-2026-00123", "", "Daniel Aguilar", "Víctor Soto"),
        ]

    def test_tono_amable_y_sin_castigo(self):
        for t in self._todos():
            bajo = t.lower()
            for prohibida in R.PALABRAS_PROHIBIDAS_EN_AVISOS:
                self.assertNotIn(prohibida, bajo, (prohibida, t))

    def test_los_titulos_son_los_que_pidio_daniel(self):
        self.assertEqual(R.titulo_aviso_nuevo("OT-2026-00123"), "Ahora estás a cargo de OT-2026-00123")
        self.assertEqual(R.titulo_aviso_anterior("OT-2026-00123", "Víctor Soto"), "OT-2026-00123 pasó a cargo de Víctor Soto")

    def test_dicen_que_ot_quien_y_con_que_cliente_y_nunca_plata(self):
        n = R.cuerpo_aviso_nuevo("OT-2026-00123", "Gimnasio X", "Daniel Aguilar", R.ORIGEN_CAMBIO, "Aarón está de vacaciones", "Aarón Rojas")
        for pieza in ("Daniel Aguilar", "OT-2026-00123", "Gimnasio X", "La llevaba Aarón Rojas", "Aarón está de vacaciones", "Puedes abrirla desde aquí"):
            self.assertIn(pieza, n)
        a = R.cuerpo_aviso_anterior("OT-2026-00123", "Gimnasio X", "Daniel Aguilar", "Víctor Soto", "Aarón está de vacaciones")
        for pieza in ("Daniel Aguilar", "Víctor Soto", "OT-2026-00123", "Gimnasio X", "no necesitas hacer nada más"):
            self.assertIn(pieza, a)
        for t in self._todos():
            self.assertIsNone(_PLATA.search(t), t)

    def test_caben_en_la_campana(self):
        for t in (R.titulo_aviso_nuevo("OT-" + "9" * 400), R.titulo_aviso_anterior("OT-" + "9" * 400, "N" * 400)):
            self.assertLessEqual(len(t), 190, "mant_notificaciones.titulo cabe en 200")
        self.assertLessEqual(len(R.cuerpo_aviso_nuevo("OT", "C" * 900, "Q" * 900, R.ORIGEN_CAMBIO, "m" * 900, "A" * 900)), 1500)

    def test_bitacora_de_la_asignacion_y_del_cambio(self):
        self.assertEqual(R.detalle_bitacora_asignado("Aarón Rojas", R.ORIGEN_CREADOR, "Aarón Rojas"), "Responsable: Aarón Rojas · es quien creó la OT")
        self.assertIn("hereda el responsable de OT-2026-00100",
                      R.detalle_bitacora_asignado("Aarón Rojas", R.ORIGEN_HEREDADO, "Daniel", "OT-2026-00100"))
        d = R.detalle_bitacora_cambio("Aarón Rojas", "Víctor Soto", "Daniel Aguilar", "vacaciones")
        self.assertEqual(d, "Responsable: Aarón Rojas → Víctor Soto · por Daniel Aguilar · Motivo: vacaciones")
        self.assertEqual(R.detalle_bitacora_cambio(None, "Víctor Soto", "Daniel Aguilar"),
                         "Responsable: sin responsable → Víctor Soto · por Daniel Aguilar")


class TestFiltroDelPanel(unittest.TestCase):
    def test_lista_blanca(self):
        self.assertEqual(R.normalizar_filtro("mis", 5), ("mis", 5))
        self.assertEqual(R.normalizar_filtro("MIS", "5"), ("mis", 5))
        self.assertEqual(R.normalizar_filtro("mis", None), ("", None), "sin sesión no hay «mis OT»")
        self.assertEqual(R.normalizar_filtro("sin", 5), ("sin", None))
        self.assertEqual(R.normalizar_filtro("12", 5), ("id", 12))
        for raro in (None, "", "todos", "12; DROP TABLE x", "-3", "0", "1.5", "  "):
            self.assertEqual(R.normalizar_filtro(raro, 5), ("", None), repr(raro))


# ═══════════════════ B) las funciones de app.py con una base de mentira ═══════════════════
class Mundo:
    """Una base de datos de mentira (nunca la real). Anota cada SQL y responde según lo que el mundo tenga."""

    def __init__(self, usuario=None):
        self.sql = []                              # [(sql, params)]
        self.columnas = True
        self.usuarios = [dict(u) for u in USUARIOS]
        self.permisos_individuales = True
        self.ejecutivos = {}                       # cliente_id -> ejecutivo_user_id
        self.visitas = {}                          # vid -> fila
        self.rowcount = 1
        self.falla = None                          # fragmento de SQL que levanta un error
        self.logs, self.avisos, self.caches = [], [], []
        self.usuario = usuario or {"id": 4, "nombre": "Juan Pablo Díaz", "username": "juanpablo@sphs.cl", "role": "supervisor"}
        self.body = {}
        self.g = types.SimpleNamespace(user=self.usuario)

    def _revisar(self, sql):
        self.sql.append((" ".join(sql.split()), None))
        if self.falla and self.falla in sql:
            raise RuntimeError("falla simulada")

    def fetchone(self, sql, params=None):
        self._revisar(sql)
        s = " ".join(sql.split())
        if "information_schema.COLUMNS" in s:
            return {"n": 4 if self.columnas else 0}
        if "SELECT ejecutivo_user_id FROM mant_clientes" in s:
            return {"ejecutivo_user_id": self.ejecutivos.get(params[0])}
        if s.startswith("SELECT numero_ot, responsable_user_id, responsable_nombre FROM mant_visitas"):
            v = self.visitas.get(params[0])
            return dict(v) if v else None
        if s.startswith("SELECT v.numero_ot, c.razon_social FROM mant_visitas"):
            v = self.visitas.get(params[0]) or {}
            return {"numero_ot": v.get("numero_ot"), "razon_social": v.get("razon_social")}
        if s.startswith("SELECT v.id, v.numero_ot, v.estado, v.responsable_user_id"):
            v = self.visitas.get(params[0])
            return dict(v) if v else None
        return None

    def fetchall(self, sql, params=None):
        self._revisar(sql)
        s = " ".join(sql.split())
        if "FROM app_users u WHERE u.active=1" in s:
            if "usuario_permisos" in s and not self.permisos_individuales:
                raise RuntimeError("no existe usuario_permisos")
            filas = [dict(u) for u in self.usuarios]
            if "usuario_permisos" not in s:
                for f in filas:
                    f["ov"] = None
            return filas
        if s.startswith("SELECT id, responsable_user_id, responsable_nombre FROM mant_visitas WHERE id IN"):
            return [dict(self.visitas[i]) for i in params if i in self.visitas]
        return []

    def escribe(self, sql, params=None):
        self.sql.append((" ".join(sql.split()), tuple(params or ())))
        if self.falla and self.falla in sql:
            raise RuntimeError("falla simulada")
        return self.rowcount

    def ambiente(self, ok=True):
        m = self
        amb = {
            "print": lambda *a, **k: None, "time": time, "datetime": dt.datetime, "_oresp": R, "g": m.g,
            "mysql_fetchone": m.fetchone, "mysql_fetchall": m.fetchall, "mysql_execute": m.escribe,
            "mysql_execute_returning_rowcount": m.escribe,
            "_mant_log": lambda ent, eid, acc, det="": m.logs.append((ent, eid, acc, det)),
            "_mant_notificar": lambda uid, tipo, titulo, cuerpo="", url_accion="", prioridad="media", cliente_id=None, visita_id=None:
                m.avisos.append({"uid": uid, "tipo": tipo, "titulo": titulo, "cuerpo": cuerpo, "url": url_accion, "prioridad": prioridad,
                                 "visita_id": visita_id, "cliente_id": cliente_id}) or 1,
            "_mant_notif_cache_invalidar": lambda uid=None: m.caches.append(uid),
            "current_username": lambda: (m.usuario or {}).get("nombre"),
            "chile_fmt_filter": lambda v, fmt="": "21/10/2026 14:05",
            "jsonify": lambda x: x, "request": types.SimpleNamespace(get_json=lambda silent=True: m.body, args={}),
            "_ot2_iniciales": lambda n: "".join(p[0] for p in (n or "").split()[:2]).upper() or None,
        }
        amb = cargar(funciones=("_rol_familia", "_ot2_err", "_ot_resp_revisar", "_ot_resp_ok", "_ot_resp_candidatos", "_ot_resp_es_candidato",
                                "_ot_responsable_por_defecto", "_ot_resp_avisar", "_ot_resp_datos_ot", "_ot_responsable_al_crear",
                                "_ot_responsable_al_crear_lote", "_ot_resp_puede_cambiar", "_ot_resp_contexto", "_ot2_panel_responsables",
                                "_ot_resp_filtro_sql", "ot_api_responsable_cambiar", "ot_api_responsables_listar", "_ensure_ot_responsable"),
                     constantes=("_OT_RESP_ESTADO", "_OT_RESP_COLUMNAS"), extra=amb)
        amb["_OT_RESP_ESTADO"]["ok"] = bool(ok)
        amb["_OT_RESP_ESTADO"]["revisado_en"] = time.time()
        return amb


def _respuesta(r):
    """(payload, http) de lo que devuelven las rutas (jsonify de mentira = el dict)."""
    return r if isinstance(r, tuple) else (r, 200)


class TestCandidatosYDefecto(unittest.TestCase):
    def test_candidatos_con_el_permiso_explicito(self):
        m = Mundo()
        c = m.ambiente()["_ot_resp_candidatos"]()
        self.assertEqual([x["nombre"] for x in c], ["Aarón Rojas", "Juan Pablo Díaz", "Víctor Soto"])
        sql = m.sql[0][0]
        self.assertIn("usuario_permisos", sql)
        self.assertIn("rol_permisos", sql)
        self.assertIn("u.active=1", sql)

    def test_sin_la_tabla_de_permisos_individuales_cae_al_permiso_del_rol(self):
        m = Mundo()
        m.permisos_individuales = False
        c = m.ambiente()["_ot_resp_candidatos"]()
        # Sin esa tabla no se conocen los permisos apagados/encendidos a mano: manda solo el del rol (Aarón y «Otro Ejecutivo»).
        self.assertEqual([x["id"] for x in c], [3, 8])

    def test_un_error_de_la_base_deja_todo_dormido_sin_romper(self):
        m = Mundo()
        m.falla = "FROM app_users"
        self.assertEqual(m.ambiente()["_ot_resp_candidatos"](), [])

    def test_se_consulta_una_sola_vez_por_peticion(self):
        m = Mundo()
        amb = m.ambiente()
        amb["_ot_resp_candidatos"]()
        amb["_ot_resp_candidatos"]()
        amb["_ot_resp_es_candidato"](3)
        self.assertEqual(len([s for s in m.sql if "FROM app_users" in s[0]]), 1)

    def test_es_candidato(self):
        amb = Mundo().ambiente()
        self.assertTrue(amb["_ot_resp_es_candidato"](3))
        self.assertTrue(amb["_ot_resp_es_candidato"]("2"))
        for no in (1, 5, 7, 8, 9, 99, None, "x"):
            self.assertFalse(amb["_ot_resp_es_candidato"](no), no)

    def test_defecto_creador_ejecutivo_o_nadie(self):
        m = Mundo()
        m.ejecutivos = {26: 3, 27: 8, 28: None}
        f = m.ambiente()["_ot_responsable_por_defecto"]
        self.assertEqual(f(26, 4), {"user_id": 4, "nombre": "Juan Pablo Díaz", "origen": "creador"})
        self.assertEqual(f(26, 7), {"user_id": 3, "nombre": "Aarón Rojas", "origen": "ejecutivo"})
        self.assertEqual(f(27, 7), {}, "el ejecutivo del cliente 27 no tiene el permiso: el asistente pide elegir")
        self.assertEqual(f(28, 7), {})
        self.assertEqual(f(None, 7), {})
        self.assertEqual(f("basura", 7), {})

    def test_el_ejecutivo_de_cada_cliente_se_consulta_una_sola_vez_en_un_lote(self):
        m = Mundo()
        m.ejecutivos = {26: 3}
        f = m.ambiente()["_ot_responsable_por_defecto"]
        memo = {}
        for _ in range(5):
            f(26, 7, memo_clientes=memo)
        self.assertEqual(len([s for s in m.sql if "FROM mant_clientes" in s[0]]), 1)

    def test_sin_candidatos_ni_se_mira_al_cliente(self):
        m = Mundo()
        m.usuarios = [u for u in m.usuarios if u["id"] in (1, 5, 7)]
        self.assertEqual(m.ambiente()["_ot_responsable_por_defecto"](26, 4), {})
        self.assertFalse([s for s in m.sql if "mant_clientes" in s[0]])


class TestFijarElResponsableAlCrear(unittest.TestCase):
    def _escrituras(self, m):
        return [s for s in m.sql if s[0].startswith("UPDATE mant_visitas")]

    def test_dormido_si_las_columnas_no_existen(self):
        m = Mundo()
        r = m.ambiente(ok=False)["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4)
        self.assertFalse(r["ok"])
        self.assertEqual(self._escrituras(m), [])
        self.assertEqual(m.logs, [])

    def test_dormido_si_nadie_tiene_el_permiso(self):
        m = Mundo()
        m.usuarios = [u for u in m.usuarios if u["id"] in (1, 5, 7)]
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7)
        self.assertEqual((r["ok"], r["aviso"]), (False, None), "no avisa ni pide nada: la función aún no se enciende")
        self.assertEqual(self._escrituras(m), [])

    def test_quien_la_crea_con_el_permiso_queda_y_no_se_avisa_a_si_mismo(self):
        m = Mundo()
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4, notificar=True, quien="Juan Pablo Díaz")
        self.assertEqual((r["ok"], r["user_id"], r["origen"]), (True, 4, "creador"))
        (sql, params), = self._escrituras(m)
        self.assertIn("responsable_user_id=%s", sql)
        self.assertIn("responsable_at=UTC_TIMESTAMP()", sql)
        self.assertIn("responsable_user_id IS NULL", sql, "solo escribe si la OT aún no tiene responsable")
        self.assertEqual(params, (4, "Juan Pablo Díaz", "Juan Pablo Díaz", 500))
        self.assertEqual(m.logs, [("visita", 500, "responsable_asignado", "Responsable: Juan Pablo Díaz · es quien creó la OT")])
        self.assertEqual(m.avisos, [])

    def test_si_el_creador_no_puede_va_el_ejecutivo_del_cliente_y_se_le_avisa_suave(self):
        m = Mundo(usuario={"id": 7, "nombre": "Administración", "username": "admin@sphs.cl", "role": "admin"})
        m.ejecutivos = {26: 3}
        m.visitas = {500: {"numero_ot": "OT-2026-00123", "razon_social": "Gimnasio X"}}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7, notificar=True)
        self.assertEqual((r["ok"], r["user_id"], r["origen"]), (True, 3, "ejecutivo"))
        self.assertIn("es el ejecutivo del cliente", m.logs[0][3])
        self.assertEqual(len(m.avisos), 1)
        a = m.avisos[0]
        self.assertEqual((a["uid"], a["tipo"], a["prioridad"], a["url"]), (3, "ot_responsable", "media", "/ot/500"))
        self.assertEqual(a["titulo"], "Ahora estás a cargo de OT-2026-00123")
        self.assertIsNone(a["visita_id"], "por url+título, para que los dos avisos de una OT no se pisen entre sí")
        self.assertIn("Gimnasio X", a["cuerpo"])
        self.assertEqual(m.caches, [3], "la campana de esa persona se refresca")

    def test_sin_notificar_no_hay_campana(self):
        m = Mundo()
        m.ejecutivos = {26: 3}
        m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7)
        self.assertEqual(m.avisos, [])

    def test_lo_que_la_persona_eligio_manda_si_es_valido(self):
        m = Mundo()
        m.ejecutivos = {26: 3}
        m.visitas = {500: {"numero_ot": "OT-2026-00123", "razon_social": "Gimnasio X"}}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7, elegido_id="2", notificar=True, quien="Administración")
        self.assertEqual((r["ok"], r["user_id"], r["origen"]), (True, 2, "elegido"))
        self.assertIn("la eligió Administración al crear la OT", m.logs[0][3])
        self.assertIn("te eligió como responsable", m.avisos[0]["cuerpo"])

    def test_si_deja_la_sugerencia_el_origen_es_el_de_la_sugerencia(self):
        m = Mundo()
        m.ejecutivos = {26: 3}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7, elegido_id=3)
        self.assertEqual((r["user_id"], r["origen"]), (3, "ejecutivo"))

    def test_un_elegido_que_no_puede_serlo_se_ignora_y_se_dice(self):
        m = Mundo()
        m.ejecutivos = {26: 3}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7, elegido_id=5)   # una técnica
        self.assertEqual((r["ok"], r["user_id"]), (True, 3))
        self.assertIn("ya no puede serlo", r["aviso"])

    def test_sin_defecto_ni_eleccion_la_ot_queda_sin_responsable_con_aviso_y_sin_escribir(self):
        m = Mundo()
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=7)
        self.assertFalse(r["ok"])
        self.assertIn("quedó sin responsable", r["aviso"])
        self.assertEqual(self._escrituras(m), [])

    def test_la_hija_hereda_el_responsable_de_su_madre_y_no_avisa(self):
        m = Mundo()
        m.visitas = {400: {"numero_ot": "OT-2026-00100", "responsable_user_id": 2, "responsable_nombre": "Víctor Soto"}}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4, madre_id=400, notificar=True)
        self.assertEqual((r["ok"], r["user_id"], r["origen"]), (True, 2, "heredado"))
        self.assertIn("hereda el responsable de OT-2026-00100", m.logs[0][3])
        self.assertEqual(m.avisos, [])

    def test_si_la_madre_no_tiene_responsable_manda_el_defecto(self):
        m = Mundo()
        m.visitas = {400: {"numero_ot": "OT-2026-00100", "responsable_user_id": None, "responsable_nombre": None}}
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4, madre_id=400)
        self.assertEqual((r["user_id"], r["origen"]), (4, "creador"))

    def test_si_otra_via_ya_lo_puso_no_se_pisa_ni_se_loguea(self):
        m = Mundo()
        m.rowcount = 0
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4)
        self.assertFalse(r["ok"])
        self.assertEqual(m.logs, [])

    def test_un_error_de_la_base_nunca_levanta_ni_bloquea_la_creacion(self):
        m = Mundo()
        m.falla = "UPDATE mant_visitas"
        r = m.ambiente()["_ot_responsable_al_crear"](500, cliente_id=26, creador_id=4)
        self.assertFalse(r["ok"])
        self.assertEqual(m.logs, [])
        self.assertFalse(Mundo().ambiente()["_ot_responsable_al_crear"]("no-es-un-id")["ok"])

    def test_el_lote_usa_la_misma_puerta_sin_campana_y_con_el_ejecutivo_consultado_una_vez(self):
        m = Mundo(usuario={"id": 7, "nombre": "Administración", "username": "admin@sphs.cl", "role": "admin"})
        m.ejecutivos = {26: 3, 27: 2}
        amb = m.ambiente()
        n = amb["_ot_responsable_al_crear_lote"]([(500, 26), (501, 26), (502, 27), (503, 99)], creador_id=7)
        self.assertEqual(n, 3)
        self.assertEqual([s[1][0] for s in self._escrituras(m)], [3, 3, 2])
        self.assertEqual(m.avisos, [], "decenas de OT no llenan la campana")
        self.assertEqual(len([s for s in m.sql if "FROM mant_clientes" in s[0] and "ejecutivo" in s[0]]), 3, "26, 27 y 99 una vez cada uno")


class TestCambioDeResponsable(unittest.TestCase):
    OT = {"id": 500, "numero_ot": "OT-2026-00123", "estado": "en_curso", "responsable_user_id": 3, "responsable_nombre": "Aarón Rojas",
          "razon_social": "Gimnasio X"}

    def _cambiar(self, usuario, body, ot=None, rowcount=1, ok=True):
        m = Mundo(usuario=usuario)
        m.visitas = {500: dict(ot or self.OT)}
        m.body = body
        m.rowcount = rowcount
        r = m.ambiente(ok=ok)["ot_api_responsable_cambiar"](500)
        return m, _respuesta(r)

    ADMIN = {"id": 7, "nombre": "Administración", "username": "admin@sphs.cl", "role": "admin"}
    DANIEL = {"id": 1, "nombre": "Daniel Aguilar", "username": "daniel.aguilar@sphs.cl", "role": "superadmin"}

    def test_la_gestion_cambia_con_motivo_y_queda_todo_registrado(self):
        m, (p, http) = self._cambiar(self.DANIEL, {"user_id": 2, "motivo": "Aarón está de vacaciones"})
        self.assertEqual(http, 200)
        self.assertTrue(p["ok"])
        self.assertEqual(p["responsable"], {"id": 2, "nombre": "Víctor Soto", "por": "Daniel Aguilar", "cuando": "21/10/2026 14:05"})
        (sql, params), = [s for s in m.sql if s[0].startswith("UPDATE mant_visitas")]
        self.assertIn("responsable_user_id <=> %s", sql, "NULL-safe: solo cambia si sigue siendo el que esa persona vio")
        self.assertEqual(params, (2, "Víctor Soto", "Daniel Aguilar", 500, 3))
        self.assertEqual(m.logs, [("visita", 500, "responsable_cambiado",
                                   "Responsable: Aarón Rojas → Víctor Soto · por Daniel Aguilar · Motivo: Aarón está de vacaciones")])

    def test_avisa_suave_a_la_persona_nueva_y_a_la_anterior(self):
        m, (p, _http) = self._cambiar(self.DANIEL, {"user_id": 2})
        por_uid = {a["uid"]: a for a in m.avisos}
        self.assertEqual(set(por_uid), {2, 3})
        self.assertEqual(por_uid[2]["titulo"], "Ahora estás a cargo de OT-2026-00123")
        self.assertEqual(por_uid[3]["titulo"], "OT-2026-00123 pasó a cargo de Víctor Soto")
        for a in m.avisos:
            self.assertEqual((a["tipo"], a["prioridad"], a["url"]), ("ot_responsable", "media", "/ot/500"))
            self.assertIsNone(_PLATA.search(a["titulo"] + " " + a["cuerpo"]))
        self.assertEqual(sorted(p["avisados"]), ["Aarón Rojas", "Víctor Soto"])

    def test_el_propio_responsable_puede_pasarle_la_ot_a_otro_y_no_se_avisa_a_si_mismo(self):
        aaron = {"id": 3, "nombre": "Aarón Rojas", "username": "aaron@sphs.cl", "role": "ejecutivo_sstt"}
        m, (p, http) = self._cambiar(aaron, {"user_id": 4})
        self.assertEqual(http, 200)
        self.assertEqual([a["uid"] for a in m.avisos], [4], "Aarón es quien lo hace: solo se avisa a Juan Pablo")

    def test_pasarse_la_ot_a_si_mismo_avisa_solo_a_la_persona_anterior(self):
        m, (p, http) = self._cambiar(self.ADMIN | {"id": 4, "nombre": "Juan Pablo Díaz", "role": "supervisor"}, {"user_id": 4})
        self.assertEqual(http, 200)
        self.assertEqual([a["uid"] for a in m.avisos], [3])

    def test_quien_no_es_gestion_ni_el_responsable_no_puede(self):
        otro = {"id": 2, "nombre": "Víctor Soto", "username": "victor@sphs.cl", "role": "ejecutivo"}
        tecnico = {"id": 5, "nombre": "Isabel Milling", "username": "isabel", "role": "tecnico_externo"}
        for u in (otro, tecnico):
            m, (p, http) = self._cambiar(u, {"user_id": 4})
            self.assertEqual((http, p["codigo"]), (403, "SIN_PERMISO"), u["role"])
            self.assertEqual([s for s in m.sql if s[0].startswith("UPDATE")], [])

    def test_el_nuevo_tiene_que_ser_un_candidato_distinto_del_actual(self):
        for body, cod, http in (({"user_id": 7}, "RESPONSABLE_NO_VALIDO", 400), ({"user_id": 5}, "RESPONSABLE_NO_VALIDO", 400),
                                ({"user_id": None}, "RESPONSABLE_REQUERIDO", 400), ({}, "RESPONSABLE_REQUERIDO", 400),
                                ({"user_id": 3}, "YA_ES_RESPONSABLE", 409)):
            m, (p, h) = self._cambiar(self.DANIEL, body)
            self.assertEqual((h, p["codigo"]), (http, cod), body)
            self.assertEqual([s for s in m.sql if s[0].startswith("UPDATE")], [])

    def test_ot_terminada_solo_el_superadmin(self):
        cerrada = dict(self.OT, estado="cerrada")
        _m, (p, http) = self._cambiar(self.ADMIN, {"user_id": 2}, ot=cerrada)
        self.assertEqual((http, p["codigo"]), (403, "SIN_PERMISO"))                      # admin sí es gestión, pero…
        gestor = {"id": 4, "nombre": "Juan Pablo Díaz", "username": "juanpablo@sphs.cl", "role": "supervisor"}
        _m, (p, http) = self._cambiar(gestor, {"user_id": 2}, ot=cerrada)
        self.assertEqual((http, p["codigo"]), (403, "SIN_PERMISO"))
        _m, (p, http) = self._cambiar(self.DANIEL, {"user_id": 2}, ot=cerrada)
        self.assertEqual(http, 200)

    def test_una_ot_sin_responsable_la_asigna_la_gestion(self):
        sin = dict(self.OT, responsable_user_id=None, responsable_nombre=None)
        m, (p, http) = self._cambiar(self.DANIEL, {"user_id": 3}, ot=sin)
        self.assertEqual(http, 200)
        self.assertEqual([a["uid"] for a in m.avisos], [3], "no hay persona anterior a quien avisar")
        self.assertEqual(m.logs[0][3], "Responsable: sin responsable → Aarón Rojas · por Daniel Aguilar")
        self.assertEqual([s for s in m.sql if s[0].startswith("UPDATE")][0][1][-1], None)

    def test_si_alguien_lo_cambio_antes_responde_conflicto_y_no_avisa(self):
        m, (p, http) = self._cambiar(self.DANIEL, {"user_id": 2}, rowcount=0)
        self.assertEqual((http, p["codigo"]), (409, "CONFLICTO"))
        self.assertEqual((m.logs, m.avisos), ([], []))

    def test_dormido_responde_503_amable_y_ot_inexistente_404(self):
        _m, (p, http) = self._cambiar(self.DANIEL, {"user_id": 2}, ok=False)
        self.assertEqual((http, p["codigo"]), (503, "NO_DISPONIBLE"))
        m = Mundo(usuario=self.DANIEL)
        m.body = {"user_id": 2}
        p, http = _respuesta(m.ambiente()["ot_api_responsable_cambiar"](999))
        self.assertEqual((http, p["codigo"]), (404, "OT_NO_EXISTE"))


class TestListaDeResponsables(unittest.TestCase):
    def test_devuelve_nombres_la_sugerencia_y_si_yo_puedo_serlo(self):
        m = Mundo(usuario={"id": 7, "nombre": "Administración", "username": "admin@sphs.cl", "role": "admin"})
        m.ejecutivos = {26: 3}
        amb = m.ambiente()
        amb["request"].args = {"cliente_id": "26"}
        p = amb["ot_api_responsables_listar"]()
        self.assertEqual(p["hay"], True)
        self.assertEqual(p["candidatos"], [{"id": 3, "nombre": "Aarón Rojas"}, {"id": 4, "nombre": "Juan Pablo Díaz"}, {"id": 2, "nombre": "Víctor Soto"}])
        self.assertEqual(p["sugerido"], {"id": 3, "nombre": "Aarón Rojas", "origen": "ejecutivo"})
        self.assertFalse(p["yo_candidato"])
        self.assertEqual(set(p), {"ok", "hay", "candidatos", "sugerido", "yo_candidato"})
        self.assertNotIn("@", json.dumps(p), "solo nombres: ni correos")

    def test_si_yo_puedo_ser_responsable_me_sugiere_a_mi(self):
        amb = Mundo().ambiente()
        p = amb["ot_api_responsables_listar"]()
        self.assertEqual((p["sugerido"]["id"], p["sugerido"]["origen"], p["yo_candidato"]), (4, "creador", True))

    def test_dormido(self):
        p = Mundo().ambiente(ok=False)["ot_api_responsables_listar"]()
        self.assertEqual((p["hay"], p["candidatos"], p["sugerido"]), (False, [], None))
        m = Mundo()
        m.usuarios = [u for u in m.usuarios if u["id"] in (1, 5, 7)]
        p = m.ambiente()["ot_api_responsables_listar"]()
        self.assertEqual((p["hay"], p["sugerido"]), (False, None))


class TestFichaDeLaOT(unittest.TestCase):
    OT = {"id": 500, "estado": "en_curso", "responsable_user_id": 3, "responsable_nombre": "Aarón Rojas", "responsable_por": "Daniel Aguilar",
          "responsable_at": dt.datetime(2026, 10, 21, 17, 5)}

    def _ctx(self, usuario, ot=None, ok=True):
        m = Mundo(usuario=usuario)
        return m.ambiente(ok=ok)["_ot_resp_contexto"](dict(ot or self.OT))

    def test_la_gestion_ve_quien_lo_asigno_y_puede_cambiar(self):
        c = self._ctx({"id": 1, "role": "superadmin"})
        self.assertEqual(c, {"id": 3, "nombre": "Aarón Rojas", "por": "Daniel Aguilar", "cuando": "21/10/2026 14:05",
                             "puede_cambiar": True, "hay_candidatos": True})

    def test_un_tecnico_ve_solo_el_nombre(self):
        c = self._ctx({"id": 5, "role": "tecnico_externo"})
        self.assertEqual(c, {"id": 3, "nombre": "Aarón Rojas", "por": None, "cuando": None, "puede_cambiar": False, "hay_candidatos": True})
        for k, v in c.items():
            self.assertNotIn("@", str(v))

    def test_un_ejecutivo_que_no_es_el_responsable_lo_ve_pero_no_lo_cambia(self):
        c = self._ctx({"id": 2, "role": "ejecutivo"})
        self.assertEqual((c["nombre"], c["puede_cambiar"]), ("Aarón Rojas", False))
        self.assertEqual(self._ctx({"id": 3, "role": "ejecutivo_sstt"})["puede_cambiar"], True)

    def test_una_ot_sin_responsable_solo_se_dibuja_para_quien_puede_asignarlo(self):
        sin = dict(self.OT, responsable_user_id=None, responsable_nombre=None)
        c = self._ctx({"id": 1, "role": "superadmin"}, sin)
        self.assertEqual((c["id"], c["nombre"], c["puede_cambiar"]), (None, "", True))
        self.assertIsNone(self._ctx({"id": 2, "role": "ejecutivo"}, sin))
        self.assertIsNone(self._ctx({"id": 5, "role": "tecnico_externo"}, sin))

    def test_dormido_no_dibuja_nada(self):
        self.assertIsNone(self._ctx({"id": 1, "role": "superadmin"}, ok=False))

    def test_una_ot_terminada_no_ofrece_cambiar_salvo_al_superadmin(self):
        cerrada = dict(self.OT, estado="cerrada")
        self.assertFalse(self._ctx({"id": 7, "role": "admin"}, cerrada)["puede_cambiar"])
        self.assertTrue(self._ctx({"id": 1, "role": "superadmin"}, cerrada)["puede_cambiar"])


class TestPanel(unittest.TestCase):
    def test_los_responsables_de_la_pagina_en_una_sola_consulta(self):
        m = Mundo()
        m.visitas = {1: {"id": 1, "responsable_user_id": 3, "responsable_nombre": "Aarón Rojas"},
                     2: {"id": 2, "responsable_user_id": None, "responsable_nombre": None}}
        filas = [{"id": 1}, {"id": 2}, {"id": 3}]
        m.ambiente()["_ot2_panel_responsables"](filas)
        self.assertEqual([(f["resp_id"], f["resp_nombre"], f["resp_ini"]) for f in filas], [(3, "Aarón Rojas", "AR"), (None, None, None), (None, None, None)])
        self.assertEqual(len([s for s in m.sql if "responsable_user_id, responsable_nombre FROM mant_visitas" in s[0]]), 1)

    def test_si_la_columna_no_existe_las_filas_quedan_sin_responsable_y_sin_consultar(self):
        m = Mundo()
        filas = [{"id": 1}, {"id": 2}]
        m.ambiente(ok=False)["_ot2_panel_responsables"](filas)
        self.assertEqual([f["resp_nombre"] for f in filas], [None, None])
        self.assertEqual([s for s in m.sql if "FROM mant_visitas" in s[0]], [])

    def test_filtro_lista_blanca_y_parametrizado(self):
        f = Mundo().ambiente()["_ot_resp_filtro_sql"]
        self.assertEqual(f("mis", 4), ("mis", "v.responsable_user_id = %s", [4]))
        self.assertEqual(f("sin", 4), ("sin", "v.responsable_user_id IS NULL", []))
        self.assertEqual(f("3", 4), ("3", "v.responsable_user_id = %s", [3]))
        for raro in ("", None, "todos", "3; DROP TABLE mant_visitas", "-1"):
            self.assertEqual(f(raro, 4), ("", "", []), repr(raro))
        self.assertEqual(f("mis", None), ("", "", []))
        self.assertEqual(Mundo().ambiente(ok=False)["_ot_resp_filtro_sql"]("mis", 4), ("", "", []), "sin la columna no filtra")


class TestArranque(unittest.TestCase):
    """REGLA #18: el arranque nunca bloquea tablas."""

    def test_cinco_sentencias_una_clausula_cada_una_por_mysql_execute(self):
        m = Mundo()
        amb = m.ambiente(ok=False)
        amb["_ensure_ot_responsable"]()
        ddl = [s[0] for s in m.sql if s[0].startswith("ALTER TABLE")]
        self.assertEqual(ddl, [
            "ALTER TABLE mant_visitas ADD COLUMN responsable_user_id INT NULL",
            "ALTER TABLE mant_visitas ADD COLUMN responsable_nombre VARCHAR(190) NULL",
            "ALTER TABLE mant_visitas ADD COLUMN responsable_at DATETIME NULL",
            "ALTER TABLE mant_visitas ADD COLUMN responsable_por VARCHAR(190) NULL",
            "ALTER TABLE mant_visitas ADD INDEX idx_v_responsable (responsable_user_id)"])
        self.assertTrue(amb["_OT_RESP_ESTADO"]["ok"], "al terminar revisa y recuerda que las columnas ya están")

    def test_la_guardia_de_ddl_las_reconoce_y_las_salta_cuando_ya_estan(self):
        amb = cargar(funciones=("_ddl_ya_aplicado",),
                     constantes=("_DDL_RE", "_DDLG_MULTI", "_DDLG_CREATE_TBL", "_DDLG_ADD_IDX", "_DDLG_CREATE_IDX", "_DDLG_ADD_COL",
                                 "_DDLG_MOD_ENUM", "_DDLG_ENUM_VAL", "_DDLG_NO_COLUMNA", "_ddl_guard_saltados"), extra={"re": re})
        guardia = amb["_ddl_ya_aplicado"]
        src = fuente_funcion("_ensure_ot_responsable")
        sentencias = re.findall(r'"(ALTER TABLE mant_visitas [^"]+)"', src)
        self.assertEqual(len(sentencias), 5)
        for s in sentencias:
            self.assertTrue(guardia(lambda sql, p: {"x": 1}, s), "ya aplicada: se salta sin pedir lock — " + s)
            self.assertFalse(guardia(lambda sql, p: None, s), "falta: se ejecuta — " + s)

    def test_sin_get_mysql_ni_lock_wait_timeout_ni_modify(self):
        src = _codigo("_ensure_ot_responsable")
        self.assertIn("mysql_execute(sql)", src)
        for prohibido in ("get_mysql", "lock_wait_timeout", "MODIFY", "UPDATE", "CHANGE", ", ADD", "DROP"):
            self.assertNotIn(prohibido, src, prohibido)

    def test_se_llama_siempre_y_dentro_de_un_try(self):
        i = APP.index("_ensure_ot_responsable()\nexcept")
        previo = APP[APP.rfind("\ntry:", 0, i):i]
        self.assertIn("with app.app_context():", previo)
        self.assertNotIn("ILUS_SKIP_MIGRATIONS", previo)
        self.assertEqual(APP.count("        _ensure_ot_responsable()\n"), 1)

    def test_no_rellena_ninguna_ot_anterior(self):
        """Ningún UPDATE masivo de mant_visitas que ponga el responsable fuera de _ot_responsable_al_crear / el cambio de una OT."""
        for m in re.finditer(r"SET responsable_user_id\s*=", APP):
            ini = APP.rfind("\ndef ", 0, m.start())
            nombre = re.match(r"\ndef (\w+)\(", APP[ini:ini + 200]).group(1)
            self.assertIn(nombre, ("_ot_responsable_al_crear", "ot_api_responsable_cambiar"), nombre)
            tramo = APP[m.start():m.start() + 400]
            self.assertIn("WHERE id=%s", tramo)
        self.assertNotIn("UPDATE mant_visitas SET responsable_user_id=%s WHERE responsable_user_id IS NULL", APP)


# ═══════════════════════════ C) el cableado en app.py ═══════════════════════════
class TestPermiso(unittest.TestCase):
    def test_llave_matriz_y_etiqueta(self):
        self.assertIn('    "mant_ot_responsable",\n)', APP, "la llave está en PERMS_KEYS (permisos individuales)")
        self.assertEqual(R.PERMISO_KEY, "mant_ot_responsable")
        i = APP.index('"mantenciones":   {"label":"Servicio Técnico"')
        self.assertIn('"ot_responsable"', APP[i:i + 1800], "aparece en Roles → Servicio Técnico")
        j = APP.index('"ot_responsable": {"label": "Responsable de OT')
        self.assertIn('"tipo": "submodulo"', APP[j:j + 300])

    def test_nace_apagado_para_todos(self):
        self.assertIn('base["mant_ot_responsable"] = bool(man.get("ot_responsable"))', APP)
        for const in ("_PERMS_CONCEDIDOS_POR_DEFECTO", "_PERMS_CONCEDIDOS_POR_OMISION"):
            self.assertNotIn("ot_responsable", fuente_constante(const), const)

    def test_los_candidatos_no_usan_g_permissions_porque_el_superadmin_las_tiene_todas(self):
        for nombre in ("_ot_resp_candidatos", "_ot_resp_es_candidato", "_ot_responsable_por_defecto", "_ot_responsable_al_crear"):
            src = fuente_funcion(nombre)
            self.assertNotIn("g.permissions", src, nombre)
            self.assertNotIn("permission_set", src, nombre)
            self.assertNotIn('"superadmin"', src, nombre)

    def test_la_seleccion_de_columnas_usa_solo_las_cuatro_del_ensure(self):
        nuevas = {"responsable_user_id", "responsable_nombre", "responsable_at", "responsable_por"}
        for nombre in ("_ot_resp_revisar", "_ot_responsable_al_crear", "_ot_resp_contexto", "_ot2_panel_responsables", "ot_api_responsable_cambiar",
                       "_ot_resp_filtro_sql"):
            usadas = set(re.findall(r"\bresponsable_[a-z_]+\b", fuente_funcion(nombre)))
            usadas -= {"responsable_asignado", "responsable_cambiado", "responsable_cambiar"}
            self.assertTrue(usadas <= nuevas, (nombre, usadas - nuevas))      # REGLA #5: verificar el CREATE TABLE antes de leer columnas


class TestCampanaEnum(unittest.TestCase):
    def test_el_tipo_del_aviso_esta_al_final_de_las_dos_listas(self):
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", APP, re.S)
        self.assertEqual(len(enums), 2)
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            self.assertIn(R.TIPO_AVISO, valores)
            self.assertEqual(valores[-1], R.TIPO_AVISO, "nuevo valor AL FINAL: no se reordena nada (REGLA #18)")
            self.assertGreater(valores.index(R.TIPO_AVISO), valores.index("ot_pendiente_stock"))
        self.assertEqual(R.TIPO_AVISO, "ot_responsable")
        self.assertLessEqual(len(R.TIPO_AVISO), 50)

    def test_el_helper_envia_ese_tipo_y_nunca_por_correo(self):
        src = _codigo("_ot_resp_avisar")
        self.assertIn("_oresp.TIPO_AVISO", src)
        self.assertNotIn("_send_ilus_email", src)
        self.assertNotIn("whatsapp", src.lower())
        for nombre in ("_ot_responsable_al_crear", "ot_api_responsable_cambiar", "_ot_resp_contexto", "_ot_responsable_al_crear_lote"):
            fuente = _codigo(nombre)
            self.assertNotIn("_send_ilus_email", fuente, nombre)
            self.assertNotIn("_mant_ot_email_cliente", fuente, nombre)        # al cliente jamás (REGLA #22)
            self.assertNotIn("notify_async", fuente, nombre)
            self.assertNotIn("whatsapp", fuente.lower(), nombre)

    def test_no_se_agrego_ningun_tipo_literal_nuevo_a__ot2_crear_core(self):
        """tests/test_ot_eventos_desinstalacion.py vigila que _ot2_crear_core solo use tipos ya existentes: el aviso vive en el helper."""
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', fuente_funcion("_ot2_crear_core")))
        self.assertNotIn(R.TIPO_AVISO, usados)
        self.assertLessEqual(usados, {"otro", "ot_asignada", "ot_pendiente_aprobacion", "ot_aprobada", "ot_rechazada"})


class TestRutas(unittest.TestCase):
    def test_decoradores(self):
        d = decoradores("ot_api_responsables_listar")
        self.assertEqual(d[1:], ["@_mant_required"])
        self.assertIn("methods=[\"GET\"]", d[0])
        d = decoradores("ot_api_responsable_cambiar")
        self.assertEqual(d[1:], ["@_mant_required", "@_ot_can_view"], "IDOR: solo quien puede ver esa OT")
        self.assertIn("methods=[\"POST\"]", d[0])

    def test_nombres_y_rutas_fuera_del_escaner_financiero(self):
        pat = re.compile(r"finanz|costo|factura|autoriz|regulariz|dashboard|facprov|margen|valoriz", re.I)
        for nombre in ("ot_api_responsables_listar", "ot_api_responsable_cambiar"):
            self.assertIsNone(pat.search(nombre), nombre)
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/responsables", methods=\["GET"\]\)', APP)), 1)
        self.assertEqual(len(re.findall(r'@app\.route\("/ot/api/<int:vid>/responsable", methods=\["POST"\]\)', APP)), 1)

    def test_la_lista_solo_trae_nombres_ids_y_la_sugerencia(self):
        src = fuente_funcion("ot_api_responsables_listar")
        self.assertNotIn("username", src)
        self.assertNotIn("correo", src)
        self.assertNotIn("phone", src)


class TestGanchosAlCrearOT(unittest.TestCase):
    """UNA función decide el responsable de TODA OT nueva: cada camino que inserta en mant_visitas la llama (o hereda)."""
    CON_GANCHO = ("_ot2_crear_core", "_mant_visita_crear_core", "mant_visita_multi", "mant_maquina_solicitar_cambio", "mant_visita_historica",
                  "mant_visita_retroactiva", "mant_intel_accion", "mant_planificador_generar_ots", "mant_generar_calendario",
                  "_mant_lev_crear_ot_core")
    # Camino -> por qué no la llama él mismo.
    EXCEPCIONES = {
        "_qp_aplicar": "la OT pendiente por falta de stock HEREDA el responsable de su madre en la misma transacción",
        "_ot_crear_visita_espejo": "la llama _mant_lev_crear_ot_core, que sí fija el responsable",
        "_mantenciones_cron_run_once": "la creación automática del cron está APAGADA de forma definitiva (batch vacío)",
        "mant_contrato_auto_calendar": "executemany: no se conocen los id de las visitas del plan masivo (sin número de OT)",
    }

    def test_todo_camino_que_inserta_una_ot_fija_el_responsable_o_tiene_su_excepcion(self):
        funciones = set()
        for m in re.finditer(r"INSERT INTO mant_visitas\b", APP):
            linea = APP[APP.rfind("\n", 0, m.start()) + 1:APP.find("\n", m.start())]
            if linea.lstrip().startswith("#"):
                continue                 # un comentario que menciona el INSERT no es un camino que crea OT
            ini = APP.rfind("\ndef ", 0, m.start())
            funciones.add(re.match(r"\ndef (\w+)\(", APP[ini:ini + 200]).group(1))
        self.assertGreaterEqual(len(funciones), 12, funciones)
        for f in sorted(funciones):
            if f in self.EXCEPCIONES:
                continue
            src = fuente_funcion(f)
            self.assertTrue("_ot_responsable_al_crear(" in src or "_ot_responsable_al_crear_lote(" in src,
                            "%s crea OT y no fija su responsable" % f)
        self.assertTrue(set(self.CON_GANCHO) <= funciones | {"_mant_lev_crear_ot_core"})
        for f in self.CON_GANCHO:
            src = fuente_funcion(f)
            self.assertIn("_ot_responsable_al_crear", src, f)

    # Cuántas llamadas REALES (no comentarios) a la puerta tiene cada camino. Una función con dos creaciones distintas (la mantención
    # histórica y su «próxima»; las dos ramas del Agente) necesita UNA llamada por cada una: con que quede una sola, la otra nace sin
    # responsable. Las dos con INSERT doble (_mant_visita_crear_core, mant_visita_multi) usan una sola: solo corre uno de los dos.
    GANCHOS_ESPERADOS = {"_ot2_crear_core": 1, "_mant_visita_crear_core": 1, "mant_visita_multi": 1, "mant_maquina_solicitar_cambio": 1,
                         "mant_visita_historica": 2, "mant_visita_retroactiva": 1, "mant_intel_accion": 2, "_mant_lev_crear_ot_core": 1}
    LOTES_ESPERADOS = {"mant_planificador_generar_ots": 1, "mant_generar_calendario": 1}

    @staticmethod
    def _llamadas(src, nombre):
        codigo = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
        return len(re.findall(r"(?<![\w.])%s\(" % re.escape(nombre), codigo))

    def test_cada_creacion_distinta_dentro_de_un_camino_llama_a_la_puerta(self):
        for f, n in self.GANCHOS_ESPERADOS.items():
            self.assertEqual(self._llamadas(fuente_funcion(f), "_ot_responsable_al_crear"), n,
                             "%s: una creación se quedaría sin responsable (o se agregó una llamada sin actualizar la prueba)" % f)
        for f, n in self.LOTES_ESPERADOS.items():
            self.assertEqual(self._llamadas(fuente_funcion(f), "_ot_responsable_al_crear_lote"), n, f)

    def test_las_excepciones_siguen_existiendo(self):
        for f in self.EXCEPCIONES:
            self.assertIn("def %s(" % f, APP, f)
        self.assertIn("responsable_user_id", fuente_funcion("_qp_aplicar"))
        self.assertIn("_ot_crear_visita_espejo(", fuente_funcion("_ot_crear_registro") + fuente_funcion("_mant_lev_crear_ot_core"))

    def test_el_gancho_va_despues_del_commit_para_no_trabarse_con_la_fila_recien_insertada(self):
        """Con otra conexión, un UPDATE sobre una fila insertada y aún sin commit esperaría a su propia transacción."""
        for f in ("_mant_visita_crear_core", "mant_visita_multi", "mant_maquina_solicitar_cambio", "mant_visita_historica",
                  "mant_visita_retroactiva", "mant_planificador_generar_ots", "mant_generar_calendario"):
            src = fuente_funcion(f)
            self.assertLess(src.index("commit()"), src.index("_ot_responsable_al_crear"), f)
        src = fuente_funcion("_ot2_crear_core")
        self.assertLess(src.index("        conn.commit()\n"), src.index("_ot_responsable_al_crear("))
        src = fuente_funcion("_mant_lev_crear_ot_core")
        self.assertLess(src.index("conn.commit()"), src.index("_ot_responsable_al_crear("))
        src = fuente_funcion("mant_intel_accion")
        self.assertLess(src.index("_conn_rv.commit()"), src.index("_ot_responsable_al_crear(_vid_rv"))
        self.assertLess(src.index("_conn_pr.commit()"), src.index("_ot_responsable_al_crear(_vid_pr"))

    def test_el_asistente_pasa_a_quien_pidio_la_ot_y_a_la_madre_de_la_desinstalacion(self):
        src = fuente_funcion("_ot2_crear_core")
        i = src.index("_ot_responsable_al_crear(")
        tramo = src[i:i + 900]
        self.assertIn('(autorizacion or {}).get("solicitado_por_user_id")', tramo, "si la aprueba Daniel, «quien crea» es quien la pidió")
        self.assertIn('madre_id=(int(madre["id"]) if es_hija else None)', tramo)
        self.assertIn('elegido_id=(None if es_hija else d.get("responsable_user_id"))', tramo)
        self.assertIn("notificar=True", tramo)
        self.assertIn("avisos.append(", src[i:i + 1300])

    def test_la_pendiente_por_falta_de_stock_hereda_nombre_y_deja_constancia(self):
        src = fuente_funcion("_qp_aplicar")
        self.assertIn('"responsable_nombre=%s", "responsable_at=UTC_TIMESTAMP()", "responsable_por=%s"', src)
        self.assertIn('"responsable_asignado"', src)
        self.assertIn("hereda el responsable de", src)

    def test_el_planificador_y_el_calendario_no_llenan_la_campana(self):
        for f in ("mant_planificador_generar_ots", "mant_generar_calendario"):
            src = fuente_funcion(f)
            self.assertIn("_ot_responsable_al_crear_lote(", src, f)
            self.assertNotIn("notificar=True", src, f)
        self.assertNotIn("notificar=True", fuente_funcion("_ot_responsable_al_crear_lote"))


class TestAvisoDeAprobacion(unittest.TestCase):
    """«OT lista para aprobar» (OT esperando tu firma de aprobación): a quién avisa lo decide _ot_gestion_destinatarios."""

    def test_usa_la_funcion_unica_y_ya_no_busca_al_creador_por_su_cuenta(self):
        src = fuente_funcion("_notificar_ot_pendiente_aprobacion_async")
        self.assertIn("_ot_gestion_destinatarios(vid)", src)
        self.assertNotIn("# 1) Creador de la OT (prioridad)", src)
        self.assertNotIn("WHERE username=%s AND active=1 LIMIT 1", src, "created_by guarda el NOMBRE, no el usuario")
        # el respaldo de siempre: supervisores/admin/superadmin se suman después y sin repetir (seen_ids)
        self.assertIn("role IN ('superadmin','admin','supervisor')", src)
        self.assertLess(src.index("_ot_gestion_destinatarios(vid)"), src.index("role IN ('superadmin','admin','supervisor')"))

    def test_el_resto_de_los_avisos_de_la_gestion_siguen_usando_la_misma_funcion(self):
        self.assertEqual(APP.count("def _ot_gestion_destinatarios("), 1)
        self.assertIn("_ot_gestion_destinatarios(vid, fila=v)", fuente_funcion("ot_api_que_paso_registrar"))
        self.assertIn("_ot_gestion_destinatarios(vid, fila=v)", fuente_funcion("ot_api_que_paso_leer"))

    def test_la_funcion_unica_sigue_poniendo_primero_al_responsable(self):
        src = fuente_funcion("_ot_gestion_destinatarios")
        self.assertLess(src.index('"responsable"'), src.index('"creador"'))
        self.assertLess(src.index('"creador"'), src.index('"ejecutivo"'))
        for escritura in ("INSERT", "UPDATE", "DELETE", "mysql_execute", "get_mysql", "_send_ilus_email", "_mant_notificar"):
            self.assertNotIn(escritura, src, escritura)

    def test_con_responsable_va_primero_y_sin_el_todo_sigue_como_hoy(self):
        """Corre la función REAL con una base de mentira: las OT anteriores (columna en NULL) no cambian."""
        usuarios = {u["id"]: u for u in USUARIOS}

        def amb(ot, ejecutivo):
            def uno(sql, params=()):
                if "FROM mant_visitas" in sql:
                    return dict(ot)
                if "FROM mant_clientes" in sql:
                    return {"ejecutivo_user_id": ejecutivo}
                if "WHERE id=%s AND active=1" in sql:
                    u = usuarios.get(params[0])
                    return dict(u) if u else None
                return None
            a = {"print": lambda *x, **k: None, "re": re, "mysql_fetchone": uno, "mysql_fetchall": lambda *x, **k: []}
            a = cargar(funciones=("_rol_familia", "_ot_gestion_destinatarios"), constantes=("_GESTION_FAMILIAS_RESPALDO", "_GESTION_CORREO_RE"),
                       extra=a)
            return a["_ot_gestion_destinatarios"]
        base = {"id": 10, "cliente_id": 5, "created_by_user_id": 4, "created_by": "Juan Pablo Díaz"}
        con = amb(dict(base, responsable_user_id=3), 2)(10)
        self.assertEqual([(p["id"], p["por"]) for p in con], [(3, "responsable"), (4, "creador"), (2, "ejecutivo")])
        sin = amb(dict(base, responsable_user_id=None), 2)(10)
        self.assertEqual([(p["id"], p["por"]) for p in sin], [(4, "creador"), (2, "ejecutivo")])
        self.assertEqual([(p["id"], p["por"]) for p in amb(dict(base), 2)(10)], [(4, "creador"), (2, "ejecutivo")], "la columna aún no existe")


class TestPanelYFichaCableados(unittest.TestCase):
    def test_el_panel_filtra_cuenta_y_viaja_el_parametro(self):
        panel = fuente_funcion("ot2_panel")
        self.assertIn('_ot_resp_filtro_sql(request.args.get("resp")', panel)
        self.assertIn("extra_where_q.append(_f_resp_sql)", panel)
        self.assertIn("_ot2_panel_responsables(filas)", panel)
        self.assertIn("f_resp=f_resp, resp_opts=resp_opts, resp_yo=resp_yo", panel)
        self.assertIn('_ORDEN_SQL["responsable"]', panel)
        self.assertNotIn("v.responsable_user_id = {", panel, "parametrizado, nunca con f-string")
        self.assertNotIn("responsable_nombre", fuente_constante("_OT2_SELECT_FILAS"), "el panel funciona aunque la columna aún no exista")

    def test_el_excel_tiene_el_mismo_filtro_y_la_columna_al_final(self):
        self.assertIn('_ot_resp_filtro_sql(args.get("resp")', fuente_funcion("_ot2_filtros_export"))
        xl = fuente_funcion("ot2_reporte_xlsx")
        self.assertIn('headers += ["Responsable de la OT"]', xl)
        self.assertIn('"v.responsable_nombre, " if _ot_resp_ok() else "NULL AS responsable_nombre, "', xl)
        self.assertLess(xl.index('headers += ["Evento"'), xl.index('headers += ["Responsable de la OT"]'), "va al final: no corre ninguna columna")

    def test_la_ficha_lo_arma_antes_de_la_barra_de_acciones(self):
        """tests/test_ot_producto_acciones.py ejecuta el bloque entre `acc_puede` y `return render_template`: lo nuevo va ANTES."""
        det = fuente_funcion("ot2_detalle")
        self.assertIn("responsable = _ot_resp_contexto(v)", det)
        self.assertLess(det.index("responsable = _ot_resp_contexto(v)"), det.index("    acc_puede = bool("))
        self.assertIn("responsable=responsable,", det)

    def test_la_bitacora_la_muestra_a_la_gestion_y_se_la_esconde_al_tecnico(self):
        det = fuente_funcion("ot2_detalle")
        self.assertIn('"responsable_asignado"', det)
        self.assertIn('"responsable_cambiado"', det)
        permitidas = fuente_constante("_ACT_TEC_PERMITIDAS")
        self.assertNotIn("responsable", permitidas, "lista blanca de la bitácora del técnico (REGLA #26)")


# ═══════════════════════════ D) plantillas renderizadas y JS ejecutado ═══════════════════════════
def _entorno_panel():
    def cargador(nombre):
        if nombre == "ot2/panel.html":
            return _leer("templates/ot2/panel.html")
        if nombre == "base.html":
            return "{% block title %}{% endblock %}<main>{% block content %}{% endblock %}</main>{% block scripts %}{% endblock %}"
        return ""        # los tres modales que incluye el panel no importan acá

    env = jinja2.Environment(loader=jinja2.FunctionLoader(cargador), extensions=["jinja2.ext.do"], undefined=jinja2.ChainableUndefined,
                             autoescape=True)      # como Flask con plantillas .html
    env.globals["url_for"] = lambda ep, **kw: "/%s?%s" % (ep, "&".join("%s=%s" % (k, v) for k, v in sorted(kw.items()) if v is not None))
    env.filters["chile_fmt"] = lambda v, f="": str(v)
    env.filters["rut_fmt"] = lambda v: str(v)
    return env


def _fila(i, **kw):
    f = {"id": i, "numero_ot": "OT-2026-%05d" % i, "numero_sufijo": "%05d" % i, "numero_prefijo": "2026", "razon_social": "Gimnasio X",
         "titulo": "Instalación", "estado": "programada", "estado_fase": "pend", "estado_label": "Programada", "estado_icon": "bi-calendar",
         "tipo_icon": "bi-tools", "tipo_label": "Instalación", "tecnico_nombre": "Lenin Urbina", "tecnico_ini": "LU", "tecnico_tipo": "interno",
         "n_tareas": 3, "n_completas": 1, "check_pct": 33, "fecha_dia": "21/10", "fecha_dow": "Mié", "tabla_semaforo": "azul",
         "tabla_semaforo_txt": "Programada", "resp_id": None, "resp_nombre": None, "resp_ini": None}
    f.update(kw)
    return f


def _panel(**kw):
    ctx = dict(filas=[], origen="reales", vista="tabla", kpis={"pend": 2, "ejec": 0, "comp": 0, "cerr": 0}, conteo_reales=2, conteo_automaticas=0,
               total_filtrado=2, q="", fase="", page=1, per_page=25, total_paginas=1, per_page_opciones=[25, 50], error=None, kanban_cols=[],
               orden="ot", dir="desc", f_tipo="", f_tec=None, f_interna="", f_anexo="", f_pago="", f_agenda="", agenda_opts=[("hoy", "Hoy")],
               tipos_opts=[], tecnicos_opts=[], f_evento=0, eventos_opts=[], es_superadmin_ot=False, permissions={"superadmin": False},
               f_resp="", resp_opts=[], resp_yo=False)
    ctx.update(kw)
    return _entorno_panel().get_template("ot2/panel.html").render(**ctx)


def _tabla(html):
    i = html.index('<table class="ot2-tabla')
    return html[i:html.index("</table>", i)]


class TestTablaRenderizada(unittest.TestCase):
    CANDS = [{"id": 3, "nombre": "Aarón Rojas"}, {"id": 4, "nombre": "Juan Pablo Díaz"}, {"id": 2, "nombre": "Víctor Soto"}]

    def _con(self, **kw):
        filas = [_fila(1, resp_id=3, resp_nombre="Aarón Rojas Quintanilla de la Fuente", resp_ini="AR"), _fila(2)]
        return _panel(filas=filas, resp_opts=self.CANDS, resp_yo=True, **kw)

    def test_columna_responsable_con_nombre_completo_y_sin_responsable_en_suave(self):
        t = _tabla(self._con())
        self.assertIn('<table class="ot2-tabla con-resp">', t)
        cabeceras = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", c)).strip() for c in re.findall(r"<th[^>]*>.*?</th>", t, re.S)]
        self.assertEqual(cabeceras, ["N° OT", "Cliente y trabajo", "Tipo", "Estado y avance", "Agenda", "Técnico", "Responsable", ""])
        celdas = re.findall(r'<td data-label="Responsable" class="o2t-resp-td">(.*?)</td>', t, re.S)
        self.assertEqual(len(celdas), 2)
        self.assertIn('<span class="o2t-av">AR</span><span class="nom">Aarón Rojas Quintanilla de la Fuente</span>', celdas[0])
        self.assertIn('<span class="o2t-sin">Sin responsable</span>', celdas[1])
        self.assertEqual(re.findall(r'<td colspan="(\d+)">', t), ["8", "8"], "7 columnas + Responsable (la fila de detalle las cubre todas)")
        self.assertNotIn("line-clamp", t)

    def test_el_orden_por_responsable_es_un_encabezado_mas(self):
        t = _tabla(self._con())
        self.assertIn("orden=responsable", t)

    def test_filtro_con_mis_ot_sin_responsable_y_cada_persona(self):
        html = self._con()
        sel = re.search(r'<select name="resp".*?</select>', html, re.S).group(0)
        opciones = re.findall(r'<option value="([^"]*)"[^>]*>(.*?)</option>', sel, re.S)
        self.assertEqual(opciones, [("", "Todos los responsables"), ("mis", "Mis OT como responsable"), ("sin", "Sin responsable"),
                                    ("3", "Aarón Rojas"), ("4", "Juan Pablo Díaz"), ("2", "Víctor Soto")])
        self.assertIn('onchange="this.form.submit()"', sel)
        self.assertIn("min-height:44px", sel)                                            # REGLA #3

    def test_mis_ot_solo_se_ofrece_a_quien_puede_ser_responsable(self):
        html = _panel(filas=[_fila(1)], resp_opts=self.CANDS, resp_yo=False)
        sel = re.search(r'<select name="resp".*?</select>', html, re.S).group(0)
        self.assertNotIn("Mis OT como responsable", sel)
        self.assertIn("Sin responsable", sel)

    def test_el_filtro_activo_se_ve_se_cuenta_y_se_limpia(self):
        html = self._con(f_resp="mis")
        self.assertIn('<option value="mis" selected>Mis OT como responsable</option>', html)
        self.assertIn("<b>mis OT como responsable</b>", html)
        self.assertIn('<span class="n">1</span>', html, "el botón «Filtros» del celular cuenta este filtro")
        self.assertIn("Limpiar", html)
        self.assertIn("&resp=mis", html, "el Reporte Excel lleva el filtro")
        self.assertIn("<b>Aarón Rojas</b>", self._con(f_resp="3"))
        self.assertIn("<b>sin responsable</b>", self._con(f_resp="sin"))

    def test_el_parametro_viaja_en_los_enlaces_de_paginado_orden_kpi_y_vistas(self):
        html = self._con(f_resp="3", page=2, total_paginas=3)
        self.assertGreaterEqual(len(re.findall(r"resp=3", html)), 14)
        for ancla in ('class="ot2-kpi k-pend', 'class="ot2-kpi k-ejec', 'class="ot2-kpi k-comp', 'class="ot2-kpi k-cerr',
                      'class="ot2-v on"', 'class="ot2-pp"', 'class="ot2-nb ', '<a class="ot2-th'):
            i = html.index(ancla)
            self.assertIn("resp=3", html[i:i + 900], ancla)
        siguiente = re.search(r'<a class="ot2-nb "\s+href="([^"]*)"', html)
        self.assertIsNotNone(siguiente)
        self.assertIn("resp=3", siguiente.group(1))

    def test_dormido_la_tabla_queda_exactamente_como_antes(self):
        html = _panel(filas=[_fila(1), _fila(2)])
        t = _tabla(html)
        self.assertIn('<table class="ot2-tabla">', t)
        self.assertNotIn("Responsable", t)
        self.assertNotIn('<select name="resp"', html)
        self.assertEqual(re.findall(r'<td colspan="(\d+)">', t), ["7", "7"])

    def test_si_nadie_tiene_el_permiso_pero_hay_filas_con_responsable_se_ve_la_columna_y_no_el_filtro(self):
        html = _panel(filas=[_fila(1, resp_id=3, resp_nombre="Aarón Rojas", resp_ini="AR")])
        self.assertIn("con-resp", _tabla(html))
        self.assertNotIn('<select name="resp"', html)

    def test_con_casilla_de_seleccion_masiva_suma_una_columna_mas(self):
        html = _panel(filas=[_fila(1)], resp_opts=self.CANDS, permissions={"superadmin": True})
        self.assertEqual(re.findall(r'<td colspan="(\d+)">', _tabla(html)), ["9"])

    def test_el_celular_tiene_su_zona_en_la_tarjeta(self):
        css = _leer("templates/ot2/panel.html")
        self.assertIn('"ag tec tec" "resp resp resp"', css)
        self.assertIn(".o2t-resp-td{grid-area:resp;border-top:1px dashed #eef0f2}", css)
        self.assertIn(".ot2-tabla td.o2t-resp-td::before", css)
        self.assertIn("th.o2t-w-resp{width:13%}", css)

    def test_el_archivo_conserva_crlf_y_parsea(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "panel.html"), "rb") as fh:
            crudo = fh.read()
        self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"))
        jinja2.Environment(extensions=["jinja2.ext.do"]).parse(crudo.decode("utf-8"))


DETALLE = _leer("templates/ot2/detalle.html")
_CHIP = re.search(r"(\{% if responsable %\}.*?\{% endif %\})\s*\{# 🆕 2026-09-02", DETALLE, re.S).group(1)
_CHIP_JS = DETALLE[DETALLE.index("window.otdAbrirResponsable = async function(){"):]
_CHIP_JS = _CHIP_JS[:_CHIP_JS.index("\n};\n") + 4]


class TestChipDeLaFicha(unittest.TestCase):
    def _render(self, responsable):
        # autoescape=True como Flask con plantillas .html: el nombre y quien lo asignó se escapan.
        return jinja2.Environment(extensions=["jinja2.ext.do"], autoescape=True).from_string(_CHIP).render(responsable=responsable)

    def test_gestion_ve_el_chip_con_cambiar_y_quien_lo_asigno(self):
        h = self._render({"id": 3, "nombre": "Aarón Rojas", "por": "Daniel Aguilar", "cuando": "21/10/2026 14:05", "puede_cambiar": True})
        self.assertIn('class="otd-d d-resp has-edit"', h)
        self.assertIn('title="Asignado por Daniel Aguilar el 21/10/2026 14:05"', h)
        self.assertIn('<span id="otdRespNombre">Aarón Rojas</span>', h)
        self.assertIn('onclick="otdAbrirResponsable()"', h)
        self.assertIn("Cambiar", h)

    def test_un_tecnico_ve_solo_el_nombre(self):
        h = self._render({"id": 3, "nombre": "Aarón Rojas", "por": None, "cuando": None, "puede_cambiar": False})
        self.assertIn('<span id="otdRespNombre">Aarón Rojas</span>', h)
        for no in ("otdAbrirResponsable", "Cambiar", "title=", "Asignado por", "has-edit"):
            self.assertNotIn(no, h, no)

    def test_sin_responsable_dice_asignar_solo_a_quien_puede(self):
        h = self._render({"id": None, "nombre": "", "por": None, "cuando": None, "puede_cambiar": True})
        self.assertIn("Sin responsable", h)
        self.assertIn("Asignar", h)
        self.assertEqual(self._render(None).strip(), "")

    def test_nombre_largo_no_se_corta(self):
        css = DETALLE[DETALLE.index(".d-resp{--dc:"):][:260]
        self.assertIn(".d-resp .v{white-space:normal;overflow:visible;text-overflow:clip", css)

    def test_el_nombre_viene_escapado(self):
        h = self._render({"id": 3, "nombre": '<img src=x onerror=alert(1)>', "por": 'x"><script>', "cuando": None, "puede_cambiar": False})
        self.assertNotIn("<img", h)
        self.assertNotIn("<script>", h)

    def test_sin_popups_nativos_en_lo_nuevo(self):
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", _CHIP_JS))
        for helper in ("ilusActionSheet(", "ilusPrompt(", "ilusToast("):
            self.assertIn(helper, _CHIP_JS)


def _node(js):
    with tempfile.TemporaryDirectory() as d:
        ruta = os.path.join(d, "t.js")
        with open(ruta, "w", encoding="utf-8") as fh:
            fh.write(js)
        r = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
    if r.returncode != 0:
        raise AssertionError("node falló:\n" + r.stderr[-2500:])
    return json.loads(r.stdout.strip().splitlines()[-1])


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestJsDeLaFicha(unittest.TestCase):
    def _correr(self, escenario):
        js_render = jinja2.Environment().from_string(_CHIP_JS).render(responsable={"id": 3}, v={"id": 500})
        cabecera = r"""
const llamadas = {toasts: [], fetches: [], hoja: null, prompt: null, recargo: false};
let servidor = {ok: true, json: {ok: true}};
let eleccion = 2, motivo = 'Aarón está de vacaciones', candidatos = [{id:3,nombre:'Aarón Rojas'},{id:4,nombre:'Juan Pablo Díaz'},{id:2,nombre:'Víctor Soto'}];
const window = {};
const document = {getElementById: () => ({textContent: 'Aarón Rojas'})};
const location = {reload: () => { llamadas.recargo = true; }};
const setTimeout = (fn) => fn();
function ilusToast(m, o){ llamadas.toasts.push([m, (o||{}).type]); }
async function ilusActionSheet(o){ llamadas.hoja = o; return eleccion; }
async function ilusPrompt(o){ llamadas.prompt = o; return motivo; }
async function fetch(url, init){
  llamadas.fetches.push([url, init ? init.body : null]);
  if (url === '/ot/api/responsables') return {json: async () => ({candidatos})};
  return {ok: servidor.ok, json: async () => servidor.json};
}
"""
        return _node(cabecera + js_render + "\n(async () => {\n" + escenario + "\nconsole.log(JSON.stringify(llamadas));\n})();")

    def test_el_js_renderizado_es_valido(self):
        js = jinja2.Environment().from_string(_CHIP_JS).render(responsable={"id": 3}, v={"id": 500})
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "t.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(js)
            r = subprocess.run(["node", "--check", ruta], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_camino_feliz_ofrece_a_los_demas_y_envia_persona_y_motivo(self):
        ll = self._correr("await window.otdAbrirResponsable();")
        self.assertEqual([o["label"] for o in ll["hoja"]["options"]], ["Juan Pablo Díaz", "Víctor Soto"], "la persona actual no se ofrece")
        self.assertIn("Aarón Rojas", ll["hoja"]["sub"])
        self.assertTrue(ll["prompt"]["multiline"])
        self.assertFalse(ll["prompt"]["required"], "el motivo es opcional")
        url, cuerpo = ll["fetches"][-1]
        self.assertEqual(url, "/ot/api/500/responsable")
        self.assertEqual(json.loads(cuerpo), {"user_id": 2, "motivo": "Aarón está de vacaciones"})
        self.assertEqual(ll["toasts"][-1], ["✓ Víctor Soto quedó a cargo de la OT", "success"])
        self.assertTrue(ll["recargo"])

    def test_motivo_vacio_se_envia_vacio(self):
        ll = self._correr("motivo = ''; await window.otdAbrirResponsable();")
        self.assertEqual(json.loads(ll["fetches"][-1][1]), {"user_id": 2, "motivo": ""})

    def test_cancelar_en_cualquier_paso_no_cambia_nada(self):
        ll = self._correr("eleccion = null; await window.otdAbrirResponsable();")
        self.assertEqual(len(ll["fetches"]), 1, "solo cargó la lista")
        ll = self._correr("motivo = null; await window.otdAbrirResponsable();")
        self.assertEqual(len(ll["fetches"]), 1)
        self.assertFalse(ll["recargo"])

    def test_sin_otra_persona_avisa_amable(self):
        ll = self._correr("candidatos = [{id:3,nombre:'Aarón Rojas'}]; await window.otdAbrirResponsable();")
        self.assertIsNone(ll["hoja"])
        self.assertIn("No hay otra persona", ll["toasts"][-1][0])

    def test_un_error_del_servidor_se_muestra_y_no_recarga(self):
        ll = self._correr("servidor = {ok: false, json: {ok: false, error: 'Alguien acaba de cambiar al responsable.'}};"
                          " await window.otdAbrirResponsable();")
        self.assertEqual(ll["toasts"][-1], ["Alguien acaba de cambiar al responsable.", "error"])
        self.assertFalse(ll["recargo"])


_MODAL = _leer("templates/ot2/_modal_crear.html")
_FRAGMENTO_RESP = _MODAL[_MODAL.index("  var _O2M_RESP_ORIGEN = {"):_MODAL.index("  function pintarTecnicos(b){")]
_COMPLETO_RESP = re.search(r"case 'responsable': return (.*?);\n", _MODAL).group(1)


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestAsistenteDeCrearOT(unittest.TestCase):
    """Corre el JS REAL del paso «Responsable» (extraído de la plantilla) con Node y un servidor de mentira."""
    CANDS = [{"id": 3, "nombre": "Aarón Rojas"}, {"id": 4, "nombre": "Juan Pablo Díaz"}, {"id": 2, "nombre": "Víctor Soto"}]

    def _correr(self, escenario):
        js = r"""
const nuevo = () => ({id:null, nombre:'', origen:'', manual:false, hay:null, cargado:false, cargando:false, error:false,
                      candidatos:[], sugerido:null, clienteKey:null});
let S = {resp: nuevo(), cliente: {id: 26}, interno: false};
let fase = 'form', pintadas = 0; const repintadas = [], pedidos = [];
const respuestas = [];
function esInterno(){ return !!S.interno; }
function esc(s){ return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function pintar(){ pintadas++; }
function repintarSeccion(k){ repintadas.push(k); }
function fetch(url){ pedidos.push(url); const r = respuestas.shift();
  return (r instanceof Error) ? Promise.reject(r) : Promise.resolve({json: () => Promise.resolve(r)}); }
const completo = () => (__COMPLETO__);
const soltar = () => new Promise(r => setImmediate(r));
__FRAGMENTO__
const SERVIDOR = {ok: true, hay: true, candidatos: __CANDS__, sugerido: {id: 3, nombre: 'Aarón Rojas', origen: 'ejecutivo'}};
(async () => {
  const out = {}; const b = {innerHTML: ''};
__ESCENARIO__
  console.log(JSON.stringify(out));
})();
""".replace("__COMPLETO__", _COMPLETO_RESP).replace("__FRAGMENTO__", _FRAGMENTO_RESP) \
           .replace("__CANDS__", json.dumps(self.CANDS)).replace("__ESCENARIO__", escenario)
        return _node(js)

    def test_el_js_es_valido(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "t.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(_FRAGMENTO_RESP)
            r = subprocess.run(["node", "--check", ruta], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_primer_pintado_pide_las_opciones_con_el_cliente_y_pone_la_sugerencia(self):
        o = self._correr("""
  respuestas.push(SERVIDOR);
  pintarResponsable(b); out.cargando = b.innerHTML.includes('Cargando'); out.completoAntes = completo();
  await soltar();
  out.pedidos = pedidos.slice(); out.resp = {id: S.resp.id, origen: S.resp.origen, manual: S.resp.manual, hay: S.resp.hay};
  out.repintadas = repintadas.slice(); out.completoDespues = completo();
  pintarResponsable(b); out.html = b.innerHTML;""")
        self.assertTrue(o["cargando"])
        self.assertFalse(o["completoAntes"], "mientras carga, el paso no está verde")
        self.assertEqual(o["pedidos"], ["/ot/api/responsables?cliente_id=26"])
        self.assertEqual(o["resp"], {"id": 3, "origen": "ejecutivo", "manual": False, "hay": True})
        self.assertEqual(o["repintadas"], ["responsable"])
        self.assertTrue(o["completoDespues"])
        h = o["html"]
        self.assertIn("Sugerido: <b>Aarón Rojas</b> — es el ejecutivo del cliente.", h)
        for n in ("Aarón Rojas", "Juan Pablo Díaz", "Víctor Soto"):
            self.assertIn(n, h)
        self.assertEqual(h.count('class="o2m-op sel"'), 1, "una sola elegida")
        self.assertEqual(len(re.findall(r'onclick="OT2C.respElegir\(\d+\)"', h)), 3)
        self.assertNotIn("line-clamp", h)
        self.assertNotIn("alert(", h)

    def test_la_persona_decide_y_volver_a_la_sugerencia_deja_de_ser_manual(self):
        o = self._correr("""
  respuestas.push(SERVIDOR); pintarResponsable(b); await soltar();
  respElegir(4); out.a = {id: S.resp.id, origen: S.resp.origen, manual: S.resp.manual};
  pintarResponsable(b); out.htmlA = b.innerHTML;
  respElegir(3); out.b = {id: S.resp.id, origen: S.resp.origen, manual: S.resp.manual};
  respElegir(999); out.c = S.resp.id;""")
        self.assertEqual(o["a"], {"id": 4, "origen": "elegido", "manual": True})
        self.assertIn("Elegido por ti", o["htmlA"])
        self.assertEqual(o["b"], {"id": 3, "origen": "ejecutivo", "manual": False})
        self.assertEqual(o["c"], 3, "un id que no es candidato no cambia nada")

    def test_cambiar_de_cliente_vuelve_a_consultar_y_respeta_lo_elegido_a_mano(self):
        o = self._correr("""
  respuestas.push(SERVIDOR); pintarResponsable(b); await soltar();
  respElegir(4);
  S.cliente = {id: 30}; respuestas.push({ok: true, hay: true, candidatos: SERVIDOR.candidatos, sugerido: null});
  pintarResponsable(b); await soltar();
  out.pedidos = pedidos.slice(); out.id = S.resp.id; out.manual = S.resp.manual;""")
        self.assertEqual(o["pedidos"], ["/ot/api/responsables?cliente_id=26", "/ot/api/responsables?cliente_id=30"])
        self.assertEqual((o["id"], o["manual"]), (4, True))

    def test_sin_sugerencia_y_sin_eleccion_se_pide_elegir_y_el_paso_queda_pendiente(self):
        o = self._correr("""
  respuestas.push({ok: true, hay: true, candidatos: SERVIDOR.candidatos, sugerido: null});
  pintarResponsable(b); await soltar(); pintarResponsable(b);
  out.id = S.resp.id; out.completo = completo(); out.html = b.innerHTML;""")
        self.assertIsNone(o["id"])
        self.assertFalse(o["completo"])
        self.assertIn("Nadie se sugiere solo para esta orden", o["html"])
        self.assertIn("Elige quién se hace cargo", o["html"])

    def test_sin_cliente_en_trabajo_interno_no_pide_el_cliente_y_el_aviso_no_lo_menciona(self):
        o = self._correr("""
  S.interno = true; S.cliente = null;
  respuestas.push({ok: true, hay: true, candidatos: SERVIDOR.candidatos, sugerido: null});
  pintarResponsable(b); await soltar(); pintarResponsable(b);
  out.pedidos = pedidos.slice(); out.html = b.innerHTML;""")
        self.assertEqual(o["pedidos"], ["/ot/api/responsables"])
        self.assertIn("quien la crea no puede ser responsable", o["html"])
        self.assertNotIn("ejecutivo", o["html"])

    def test_nadie_tiene_el_permiso_el_paso_desaparece_y_no_bloquea(self):
        o = self._correr("""
  respuestas.push({ok: true, hay: false, candidatos: [], sugerido: null});
  pintarResponsable(b); await soltar();
  out.pintadas = pintadas; out.hay = S.resp.hay; out.completo = completo(); out.repintadas = repintadas.slice();""")
        self.assertEqual((o["hay"], o["completo"]), (False, True))
        self.assertEqual(o["pintadas"], 1, "se vuelve a pintar el formulario SIN el paso")
        self.assertEqual(o["repintadas"], [])

    def test_si_falla_la_red_no_bloquea_la_creacion_y_lo_dice(self):
        o = self._correr("""
  respuestas.push(new Error('sin red'));
  pintarResponsable(b); await soltar(); pintarResponsable(b);
  out.error = S.resp.error; out.completo = completo(); out.html = b.innerHTML;""")
        self.assertTrue(o["error"])
        self.assertTrue(o["completo"])
        self.assertIn("La OT se crea igual", o["html"])

    def test_dos_cambios_seguidos_de_cliente_no_dejan_peticiones_colgadas(self):
        o = self._correr("""
  respuestas.push(SERVIDOR);
  pintarResponsable(b); pintarResponsable(b); pintarResponsable(b);
  out.pedidos = pedidos.length; await soltar();""")
        self.assertEqual(o["pedidos"], 1, "una sola petición mientras se espera la respuesta")

    def test_el_nombre_viene_escapado(self):
        o = self._correr("""
  respuestas.push({ok: true, hay: true, candidatos: [{id: 3, nombre: '<img src=x onerror=alert(1)>'}], sugerido: null});
  pintarResponsable(b); await soltar(); pintarResponsable(b); out.html = b.innerHTML;""")
        self.assertNotIn("<img", o["html"])
        self.assertIn("&lt;img", o["html"])


class TestAsistenteCableado(unittest.TestCase):
    def test_el_paso_va_entre_cliente_y_detalle_y_solo_si_alguien_puede_serlo(self):
        i = _MODAL.index("function pasos(){")
        src = _MODAL[i:i + 4200]
        self.assertLess(src.index("p.push({k:'cliente', n:'Cliente'})"), src.index("p.push({k:'responsable', n:'Responsable'})"))
        self.assertLess(src.index("p.push({k:'responsable', n:'Responsable'})"), src.index("p.push({k:'detalle', n:'Detalle'})"))
        self.assertIn("if (S.resp && S.resp.hay !== false) p.push", src)

    def test_se_pinta_se_repinta_se_exporta_y_viaja_en_el_cuerpo(self):
        self.assertIn("if (p.k==='responsable') pintarResponsable(b);", _MODAL)
        self.assertIn("if (k==='responsable') pintarResponsable(b);", _MODAL)
        self.assertIn("respElegir:respElegir,", _MODAL)
        self.assertIn("responsable_user_id: (S.resp && S.resp.id) ? S.resp.id : null,", _MODAL)
        self.assertIn("repintarSeccion('responsable');   // 🙋 2026-10-21: la sugerencia", _MODAL)
        self.assertIn("if (!S.resp.cargado && !S.resp.cargando) _o2mRespCargar(_o2mRespKey());", _MODAL)

    def test_reset_crea_un_estado_nuevo_en_cada_apertura(self):
        self.assertIn("resp:{id:null, nombre:'', origen:'', manual:false, hay:null, cargado:false, cargando:false, error:false,", _MODAL)

    def test_lo_nuevo_cumple_las_reglas_de_diseno(self):
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", _FRAGMENTO_RESP))                  # REGLA #1
        self.assertNotIn("o2t-", _FRAGMENTO_RESP)                                                              # tests de la tabla v2
        self.assertIn("o2m-op", _FRAGMENTO_RESP)                                                               # el mismo lenguaje de Técnicos
        self.assertIn("o2m-aviso", _FRAGMENTO_RESP)                                                            # ámbar suave, nunca rojo
        self.assertNotIn("text-overflow", _FRAGMENTO_RESP)
        self.assertNotIn("line-clamp", _FRAGMENTO_RESP)                                                        # nombres completos (REGLA #15)

    def test_el_archivo_conserva_crlf_y_parsea(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), "rb") as fh:
            crudo = fh.read()
        self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"))
        jinja2.Environment(extensions=["jinja2.ext.do"]).parse(crudo.decode("utf-8"))


class TestLaReglaEstaDocumentada(unittest.TestCase):
    def test_claude_md_tiene_la_regla_36(self):
        md = _leer("CLAUDE.md")
        i = md.index("## 🙋 REGLA #36")
        regla = md[i:]
        for pieza in ("Responsable de OT", "_ot_responsable_al_crear", "_ot_gestion_destinatarios", "tests/test_ot_responsable.py",
                      "Permisos individuales", "NO se rellenan", "REGLA #22"):
            self.assertIn(pieza, regla, pieza)
        self.assertLess(i, md.index("_Mantenedor:"), "va antes del pie")


if __name__ == "__main__":
    unittest.main()

"""DESINSTALACIÓN — correcciones de la REVISIÓN ADVERSARIAL del 20-oct-2026 (reglas #24, #25, #28, #29, #26 + interfaz).

2026-10-21: Daniel pidió eliminar la función «Eventos» («el de eventos no me sirve, elimínalo y el código»): se quitaron de aquí las
pruebas de eventos (excepción del «evento con fecha de fin», avance, estado, pantallas, formulario). Lo de la desinstalación sigue.

Cada prueba reproduce el escenario que encontró la revisión y comprueba que ya no ocurre. Sin base de datos ni navegador: la
lógica pura (ot_eventos.py), las funciones de app.py que se EJECUTAN con dobles de la base (tests/_fuente_app.py), la bandera SQL de
la madre válida contra un sqlite en memoria, y vigilancia por texto de lo que no se puede ejecutar aquí.

Correr (por archivo, la suite completa no sirve en esta máquina):
    py -m pytest tests/test_ot_eventos_revision_20oct.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import sqlite3
import unittest

import ot_eventos as E
import ot_saldo_servicio as S
from tests import _fuente_app as F

D = dt.date
SIN_PRINT = {"print": lambda *a, **k: None}


def madre(**kw):
    base = {"id": 10, "numero_ot": "OT-2026-00350", "tipo": "instalacion", "estado": "programada", "cliente_id": 5,
            "ot_madre_id": None, "desinstalacion_prevista": D(2026, 11, 26), "fecha_programada": D(2026, 10, 26),
            "fecha_fin": D(2026, 10, 27), "hora_fin": "13:00", "fecha_realizada": None, "evento_id": None,
            "modalidad_cobro": "pagado", "cubierto_por": "cliente", "cobro_cero_motivo": None}
    base.update(kw)
    return base


def hija(**kw):
    base = {"id": 11, "numero_ot": "OT-2026-00351", "tipo": "desinstalacion", "estado": "programada", "cliente_id": 5,
            "ot_madre_id": 10, "ot_relacion": "desinstalacion", "fecha_programada": D(2026, 11, 26),
            "hora_inicio": "09:00", "firma_tecnico_at": None, "hora_real_inicio": None, "fecha_realizada": None}
    base.update(kw)
    return base


def leer(*partes):
    with open(os.path.join(F.RAIZ, *partes), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA1_MadreQueNoSeCobraNoCubre(unittest.TestCase):
    """ALTO 1: una instalación en $0 (garantía, regalía, arriendo/leasing) no cubre a su desinstalación: sería un segundo $0 sin
    autorización y sin candado de pérdida (REGLA #24: «un $0 aprobado vale solo para LA OT para la que se pidió»)."""

    def test_madre_cobra(self):
        self.assertTrue(E.madre_cobra(madre()))
        self.assertTrue(E.madre_cobra({}))                                   # una clave que no viene = caso normal
        self.assertTrue(E.madre_cobra(madre(cubierto_por="contrato")))        # el default de la columna no es un $0
        self.assertFalse(E.madre_cobra(madre(modalidad_cobro="garantia")))
        self.assertFalse(E.madre_cobra(madre(modalidad_cobro="sin_costo")))
        self.assertFalse(E.madre_cobra(madre(modalidad_cobro="interno")))
        self.assertFalse(E.madre_cobra(madre(cubierto_por="garantia")))
        for motivo in ("garantia", "regalia", "arriendo_leasing"):
            self.assertFalse(E.madre_cobra(madre(cobro_cero_motivo=motivo)), motivo)

    def test_madre_valida_para_cubrir_rechaza_el_cero(self):
        ok, _ = E.madre_valida_para_cubrir(madre(), 5, tiene_doc_real=True)
        self.assertTrue(ok)
        for kw in ({"modalidad_cobro": "garantia"}, {"cobro_cero_motivo": "arriendo_leasing"}, {"cobro_cero_motivo": "regalia"},
                   {"modalidad_cobro": "sin_costo"}, {"cubierto_por": "garantia"}):
            ok, motivo = E.madre_valida_para_cubrir(madre(**kw), 5, tiene_doc_real=True)
            self.assertFalse(ok, kw)
            self.assertIn("no se cobra", motivo)
        # ni siquiera con la autorización que la incluía: el $0 de la instalación no se hereda
        ok, _ = E.madre_valida_para_cubrir(madre(modalidad_cobro="garantia"), 5, tiene_doc_real=True, aut_aprobada=True)
        self.assertFalse(ok)

    def test_hija_de_una_madre_en_arriendo_no_es_cubierta(self):
        self.assertTrue(E.es_hija_cubierta(hija(), True))
        self.assertFalse(E.es_hija_cubierta(hija(), E.madre_valida_para_cubrir(madre(cobro_cero_motivo="arriendo_leasing"), 5, True)[0]))

    def test_la_bandera_sql_de_la_madre_valida(self):
        """La MISMA bandera que lee _ot_cobertura en listas, cierre y Monitor, ejecutada contra un sqlite en memoria."""
        flag = F.cargar(constantes=("_OT_FIN_SQL_MADRE_VALIDA",))["_OT_FIN_SQL_MADRE_VALIDA"]
        con = sqlite3.connect(":memory:")
        con.executescript("""
            CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, tipo TEXT, estado TEXT, cliente_id INT, ot_madre_id INT,
                ot_relacion TEXT, desinstalacion_prevista TEXT, desinstalacion_aut_id INT, modalidad_cobro TEXT,
                cubierto_por TEXT, cobro_cero_motivo TEXT);
            CREATE TABLE mant_visita_documentos (id INTEGER PRIMARY KEY, visita_id INT, origen TEXT, es_cobro INT,
                reemplazado_por_id INT, heredado_de_visita_id INT, erp_tido TEXT);
            CREATE TABLE mant_ot_autorizaciones (id INTEGER PRIMARY KEY, estado TEXT, tipo TEXT, visita_creada_id INT, visita_id INT);
        """)

        def caso(madre_kw=None, docs=(), auts=()):
            con.execute("DELETE FROM mant_visitas"); con.execute("DELETE FROM mant_visita_documentos")
            con.execute("DELETE FROM mant_ot_autorizaciones")
            m = {"id": 10, "tipo": "instalacion", "estado": "programada", "cliente_id": 5, "ot_madre_id": None,
                 "ot_relacion": None, "desinstalacion_prevista": "2026-11-26", "desinstalacion_aut_id": None,
                 "modalidad_cobro": "pagado", "cubierto_por": "cliente", "cobro_cero_motivo": None}
            m.update(madre_kw or {})
            con.execute("INSERT INTO mant_visitas VALUES (:id,:tipo,:estado,:cliente_id,:ot_madre_id,:ot_relacion,"
                        ":desinstalacion_prevista,:desinstalacion_aut_id,:modalidad_cobro,:cubierto_por,:cobro_cero_motivo)", m)
            con.execute("INSERT INTO mant_visitas VALUES (11,'desinstalacion','programada',5,10,'desinstalacion',NULL,NULL,'pagado','cliente',NULL)")
            for d in docs:
                con.execute("INSERT INTO mant_visita_documentos (visita_id,origen,es_cobro,reemplazado_por_id,heredado_de_visita_id,erp_tido) "
                            "VALUES (?,?,?,?,?,?)", d)
            for a in auts:
                con.execute("INSERT INTO mant_ot_autorizaciones (id,estado,tipo,visita_creada_id,visita_id) VALUES (?,?,?,?,?)", a)
            return con.execute("SELECT " + flag + " AS mv FROM mant_visitas v WHERE v.id=11").fetchone()[0]

        doc_fcv = [(10, "erp", 1, None, None, "FCV")]
        self.assertTrue(caso(docs=doc_fcv))
        self.assertTrue(caso(docs=[(10, "erp", 1, None, None, "NVV")]))
        # ── A1: el $0 de la instalación no se hereda
        self.assertFalse(caso({"modalidad_cobro": "garantia"}, docs=doc_fcv))
        self.assertFalse(caso({"modalidad_cobro": "sin_costo"}, docs=doc_fcv))
        self.assertFalse(caso({"cobro_cero_motivo": "arriendo_leasing"}, docs=doc_fcv))
        self.assertFalse(caso({"cobro_cero_motivo": "regalia"}, docs=doc_fcv))
        self.assertFalse(caso({"cubierto_por": "garantia"}, docs=doc_fcv))
        # ── lo de siempre: documento real, vigente y propio
        self.assertFalse(caso(docs=[]))
        self.assertFalse(caso(docs=[(10, "erp", 0, None, None, "FCV")]))               # es_cobro=0
        self.assertFalse(caso(docs=[(10, "erp", 1, 99, None, "FCV")]))                 # dado de baja
        self.assertFalse(caso(docs=[(10, "erp", 1, None, 77, "FCV")]))                 # espejo heredado
        self.assertFalse(caso(docs=[(10, "cotizacion", 1, None, None, "FCV")]))        # no es de Random
        self.assertFalse(caso({"estado": "cancelada"}, docs=doc_fcv))
        self.assertFalse(caso({"desinstalacion_prevista": None}, docs=doc_fcv))
        self.assertFalse(caso({"cliente_id": 99}, docs=doc_fcv))
        self.assertFalse(caso({"tipo": "correctiva"}, docs=doc_fcv))
        self.assertFalse(caso({"ot_madre_id": 3}, docs=doc_fcv))

    def test_a6_solo_la_solicitud_que_incluia_la_desinstalacion_reemplaza_el_documento(self):
        """MEDIO 6: la autorización 'desinstalacion_cubierta' solo permite declarar la desinstalación DESPUÉS; no reemplaza el
        documento de Random. La única excepción es la 'crear_sin_documento' de ESA instalación."""
        flag = F.cargar(constantes=("_OT_FIN_SQL_MADRE_VALIDA",))["_OT_FIN_SQL_MADRE_VALIDA"]
        con = sqlite3.connect(":memory:")
        con.executescript("""
            CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, tipo TEXT, estado TEXT, cliente_id INT, ot_madre_id INT,
                ot_relacion TEXT, desinstalacion_prevista TEXT, desinstalacion_aut_id INT, modalidad_cobro TEXT,
                cubierto_por TEXT, cobro_cero_motivo TEXT);
            CREATE TABLE mant_visita_documentos (id INTEGER PRIMARY KEY, visita_id INT, origen TEXT, es_cobro INT,
                reemplazado_por_id INT, heredado_de_visita_id INT, erp_tido TEXT);
            CREATE TABLE mant_ot_autorizaciones (id INTEGER PRIMARY KEY, estado TEXT, tipo TEXT, visita_creada_id INT, visita_id INT);
            INSERT INTO mant_visitas VALUES (10,'instalacion','programada',5,NULL,NULL,'2026-11-26',77,'pagado','cliente',NULL);
            INSERT INTO mant_visitas VALUES (11,'desinstalacion','programada',5,10,'desinstalacion',NULL,NULL,'pagado','cliente',NULL);
        """)

        def con_aut(estado, tipo, creada, visita):
            con.execute("DELETE FROM mant_ot_autorizaciones")
            con.execute("INSERT INTO mant_ot_autorizaciones VALUES (77,?,?,?,?)", (estado, tipo, creada, visita))
            return con.execute("SELECT " + flag + " FROM mant_visitas v WHERE v.id=11").fetchone()[0]
        self.assertTrue(con_aut("aprobada", "crear_sin_documento", 10, None))
        self.assertFalse(con_aut("aprobada", "desinstalacion_cubierta", None, 10), "declarar la desinstalación no respalda nada sin documento")
        self.assertFalse(con_aut("aprobada", "crear_sin_documento", 55, None), "la autorización es de OTRA instalación")
        self.assertFalse(con_aut("pendiente", "crear_sin_documento", 10, None))
        self.assertFalse(con_aut("aprobada", "cobro_cero", None, 10))

    def test_la_fila_basica_trae_lo_que_hace_falta(self):
        cols = F.fuente_constante("_OT_FILA_BASICA_COLS")
        for c in ("modalidad_cobro", "cubierto_por", "cobro_cero_motivo", "firma_tecnico_at", "hora_real_inicio"):
            self.assertIn(c, cols)
        for c in ("evento_id", "evento_fin_al_crear"):
            self.assertNotIn(c, cols)

    def test_aut_incluia_lee_el_tipo_y_la_instalacion(self):
        visto = {}

        def fetch(sql, params):
            visto["sql"], visto["params"] = sql, params
            return {"x": 1}
        a = F.cargar(["_ot_madre_aut_incluia"], extra=dict(SIN_PRINT, mysql_fetchone=fetch))
        self.assertTrue(a["_ot_madre_aut_incluia"]({"id": 10, "desinstalacion_aut_id": 77}))
        self.assertIn("tipo='crear_sin_documento'", visto["sql"])
        self.assertIn("visita_creada_id=%s", visto["sql"])
        self.assertIn("estado='aprobada'", visto["sql"])
        self.assertEqual(visto["params"], (77, 10))
        self.assertFalse(a["_ot_madre_aut_incluia"]({"id": 10, "desinstalacion_aut_id": None}))
        self.assertFalse(a["_ot_madre_aut_incluia"](None))
        a2 = F.cargar(["_ot_madre_aut_incluia"], extra=dict(SIN_PRINT, mysql_fetchone=lambda s, p: None))
        self.assertFalse(a2["_ot_madre_aut_incluia"]({"id": 10, "desinstalacion_aut_id": 77}))

    def test_perdida_cuando_la_instalacion_no_se_cobra_se_evalua_cada_hija_sola(self):
        """ALTO 1 (cierre): antes `_fac_perdida_de_ot(hija)` se anclaba en la instalación, devolvía «no cobra» y la pérdida de la
        desinstalación que SÍ cobra su línea no se revisaba nunca."""
        cobran = {10: False, 11: True}

        def build():
            a = F.cargar(["_fac_perdida_de_ot"], extra=dict(
                SIN_PRINT, _fac_familia_de=lambda vid: {"madre_id": 10, "hijas": [{"id": 11}], "ids": [10, 11]},
                mysql_fetchone=lambda sql, p: {"id": p[0]}, _ot_fin_cols_sql=lambda alias: "x",
                _ot_repuestos_desglose=lambda ids: {}, _ot_fin_rep_liviano=lambda x: None,
                _ot_finanzas=lambda v, rep=None: {"cobra": cobran[v["id"]], "cobre": {"total": 90000, "hay": True},
                                                   "me_cobraron": {"tecnico": 100000, "despacho": 0}},
                _fac_familia_cuenta=lambda fam: [{"rol": "instalacion", "cobre": 0, "tecnico": 0, "despacho": 0, "repuestos": 0},
                                                 {"rol": "desinstalacion", "cobre": 90000, "tecnico": 100000, "despacho": 0, "repuestos": 0}],
                _fac_perdida_info=lambda ancla, fin, familia=None: {"ancla": ancla, "con_familia": familia is not None, "cobra": True}))
            return a["_fac_perdida_de_ot"]
        r = build()(11)
        self.assertEqual((r["ancla"], r["con_familia"]), (11, False), "la hija que cobra se evalúa SOLA")
        r = build()(10)
        self.assertFalse(r["cobra"], "la instalación que no se cobra no tiene pérdida que revisar")
        cobran[10] = True   # si la instalación sí se cobra, vuelve la cuenta de la familia anclada en ella
        r = build()(11)
        self.assertEqual((r["ancla"], r["con_familia"]), (10, True))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA2_DeclararDespuesSiemprePideAutorizacion(unittest.TestCase):
    """ALTO 2 (versión 2026-10-21): antes la excepción de la autorización 'desinstalacion_cubierta' valía si la instalación NACIÓ en
    un evento con fecha de fin. Eventos se eliminó: declarar la desinstalación DESPUÉS de crear la instalación pide SIEMPRE la
    autorización (salvo que se haya respondido «Sí» en el asistente)."""

    def test_sin_excepcion_de_evento(self):
        self.assertTrue(E.necesita_autorizacion_prevista(False))
        self.assertFalse(E.necesita_autorizacion_prevista(True))
        self.assertFalse(hasattr(E, "guardia_evento_de_hija"))

    def test_el_servidor_ya_no_tiene_la_marca_ni_la_funcion(self):
        t = F.texto()
        self.assertNotIn("_ot_madre_evento_fin", t)
        self.assertNotIn("evento_fin_al_crear", t)
        for fn in ("_ot_desinst_contexto", "ot_api_desinstalacion_programar", "ot_api_desinstalacion_prevista"):
            self.assertIn("necesita_autorizacion_prevista(False)", F.fuente_funcion(fn), fn)

    def test_migracion_barata_sin_columnas_de_evento(self):
        src = F.fuente_funcion("_ensure_ot_eventos_cols")
        self.assertNotIn("evento_fin_al_crear", src.split('"""', 2)[2])   # ni en las sentencias
        self.assertIn("ADD COLUMN ot_madre_id INT NULL", src)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA3_EspejoYAsociarFactura(unittest.TestCase):
    """ALTO 3: el documento espejado de la madre no puede quedarse con la factura que la hija asocia ni sacar su consumo del saldo."""

    class Bd:
        """Una tabla mant_visita_documentos de mentira con las pocas consultas que hace _ot_doc_registrar_fila."""

        def __init__(self, filas):
            self.filas, self.n = [dict(f) for f in filas], max([f["id"] for f in filas] or [0])
            self.updates, self.logs = [], []

        @staticmethod
        def _nudo(x):
            return str(x or "").lstrip("0")

        def fetchone(self, sql, params=()):
            if "heredado_de_visita_id IS NULL" in sql and sql.lstrip().startswith("SELECT id FROM mant_visita_documentos WHERE visita_id=%s AND origen='erp'"):
                vid, tido, nudo = params
                for f in self.filas:
                    if f["visita_id"] == vid and f["tido"] == tido and self._nudo(f["nudo"]) == self._nudo(nudo) and f["heredado"] is None:
                        return {"id": f["id"]}
                return None
            if "ORDER BY id DESC" in sql:
                vid, tido, nudo = params
                m = [f for f in self.filas if f["visita_id"] == vid and f["tido"] == tido and f["nudo"] == nudo]
                return {"id": m[-1]["id"]} if m else None
            return None

        def execute(self, sql, params=()):
            if sql.lstrip().startswith("INSERT INTO mant_visita_documentos"):
                self.n += 1
                self.filas.append({"id": self.n, "visita_id": params[0], "tido": params[1], "nudo": params[2], "heredado": None})
            else:
                self.updates.append((sql, params))

        def execute_rowcount(self, sql, params=()):
            vid, tido, nudo = params
            antes = len(self.filas)
            self.filas = [f for f in self.filas if not (f["visita_id"] == vid and f["tido"] == tido
                                                       and self._nudo(f["nudo"]) == self._nudo(nudo) and f["heredado"] is not None)]
            return antes - len(self.filas)

    def cargar(self, bd):
        return F.cargar(["_ot_doc_registrar_fila"], extra=dict(
            SIN_PRINT, current_username=lambda: "aaron", mysql_fetchone=bd.fetchone, mysql_execute=bd.execute,
            mysql_execute_returning_rowcount=bd.execute_rowcount, _mant_log=lambda *a, **k: bd.logs.append(a),
            _erp_fecha_a_date=lambda x: None, _OT_DOCS_COBRO=("FCV", "FCE", "BLV", "BLE"), _OT_DOCS_NOTA_VENTA=("NVV", "NVI")))

    def test_la_factura_asociada_deja_fila_propia_y_retira_el_espejo(self):
        bd = self.Bd([
            {"id": 1, "visita_id": 11, "tido": "NVV", "nudo": "VD00010961", "heredado": None},   # su nota de venta (lleva su aporte)
            {"id": 2, "visita_id": 11, "tido": "FCV", "nudo": "11500", "heredado": 10},          # espejo de la factura de la madre
        ])
        self.cargar(bd)["_ot_doc_registrar_fila"](11, "FCV", "11500", 500000, "76.000.000-0", True, None, None, None)
        propios = [f for f in bd.filas if f["tido"] == "FCV" and f["heredado"] is None]
        self.assertEqual(len(propios), 1, "la factura queda como documento PROPIO de la hija")
        self.assertEqual([f for f in bd.filas if f["heredado"] is not None], [], "la copia espejada se retira")
        # la nota de venta propia queda dada de baja por la fila PROPIA (no por el espejo ya borrado)
        baja = [p for (s, p) in bd.updates if "reemplazado_por_id=%s" in s]
        self.assertEqual(baja[0][0], propios[0]["id"])
        self.assertTrue(any(a[2] == "documento_heredado_reemplazado" for a in bd.logs))

    def test_sin_espejo_sigue_igual(self):
        bd = self.Bd([])
        self.cargar(bd)["_ot_doc_registrar_fila"](5, "FCV", "1", 1, None, True, None, None, None)
        self.assertEqual(len(bd.filas), 1)
        self.assertFalse(any(a[2] == "documento_heredado_reemplazado" for a in bd.logs))

    def test_un_documento_propio_ya_asociado_no_se_duplica(self):
        bd = self.Bd([{"id": 1, "visita_id": 5, "tido": "FCV", "nudo": "11500", "heredado": None}])
        self.cargar(bd)["_ot_doc_registrar_fila"](5, "FCV", "11500", 1, None, True, None, None, None)
        self.assertEqual(len(bd.filas), 1)

    def test_la_fuente_3_del_saldo_ignora_los_espejos(self):
        src = F.fuente_funcion("_ot_saldo_usos")
        i = src.index("NOT EXISTS (SELECT 1 FROM mant_visita_documentos x")
        self.assertIn("x.heredado_de_visita_id IS NULL", src[i:i + 300])

    def test_no_se_espeja_en_una_hija_que_cobra_su_linea(self):
        espejados = []

        class Cur:
            def __init__(self, cobran):
                self.cobran, self.ult = cobran, None

            def execute(self, sql, params=()):
                self.ult = params[0] if params else None

            def fetchone(self):
                return {"x": 1} if self.ult in self.cobran else None

            def __enter__(self): return self
            def __exit__(self, *a): return False

        class Conn:
            def __init__(self, cobran): self.cobran = cobran
            def cursor(self): return Cur(self.cobran)
            def commit(self): pass
            def close(self): pass
        a = F.cargar(["_ot_resincronizar_espejo"], extra=dict(
            SIN_PRINT, _evt=E, _ot_hijas_de=lambda mid: [hija(id=11), hija(id=12)], _ot_fila_basica=lambda v: madre(),
            get_mysql=lambda: Conn({11}),
            _ot_espejar_docs_en_hija=lambda cur, hid, mid, num: espejados.append(hid) or 1))
        self.assertEqual(a["_ot_resincronizar_espejo"](10), 1)
        self.assertEqual(espejados, [12], "la hija que cobra su propia línea (11) no recibe espejos")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA4_ArgumentoDeLaCubiertaQuedaGuardado(unittest.TestCase):
    """ALTO 4 (REGLA #24 del 18-oct): el argumento de «cubierta» con saldo libre se guarda con quién, cuándo y por qué."""

    def test_la_clasificacion_de_la_hija_lleva_el_argumento(self):
        guardado = {}

        class Cur:
            def execute(self, sql, params=()):
                if sql.startswith("UPDATE mant_visitas SET fin_clasif"):
                    guardado["codigo"], guardado["json"] = params[0], json.loads(params[1])
        a = F.cargar(["_ot_fin_clasif_guardar"], extra=dict(
            SIN_PRINT, _saldo=S, json=json, datetime=dt.datetime, current_username=lambda: "aaron",
            chile_fmt_filter=lambda v, fmt=None: "20/10/2026 10:00"))
        arg = "El mall absorbe la desinstalación de la trotadora dentro del precio de la instalación."
        campos = {"cobertura": "cubierta", "madre_id": 10, "madre_numero": "OT-2026-00350", "argumento_cubierta": arg,
                  "documentos_validados": [], "valor_origen": None}
        a["_ot_fin_clasif_guardar"](Cur(), 11, campos, False, None)
        self.assertEqual(guardado["codigo"], "cubierta")
        self.assertEqual(guardado["json"]["argumento"], arg)
        self.assertEqual(guardado["json"]["registrado_por"], "aaron")
        self.assertTrue(guardado["json"]["registrado_at"])
        self.assertIn("OT-2026-00350", guardado["json"]["texto"])

    def test_el_nucleo_guarda_el_argumento_y_deja_constancia_en_la_madre(self):
        nucleo = F.fuente_funcion("_ot2_crear_core")
        self.assertIn('_fin_campos["argumento_cubierta"] = ', nucleo)
        self.assertIn('d.get("_desinst_argumento")', nucleo)
        self.assertIn('"desinstalacion_cubierta_con_saldo"', nucleo)
        # esa acción de la bitácora NO es visible para un técnico (lista blanca, REGLA #26)
        permitidas = F.cargar(constantes=("_ACT_TEC_PERMITIDAS",))["_ACT_TEC_PERMITIDAS"]
        self.assertNotIn("desinstalacion_cubierta_con_saldo", permitidas)
        self.assertNotIn("reserva_saldo_liberada", permitidas)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class _AyudasValidarPrevio:
    """Lo que comparten las pruebas de _ot_desinst_validar_previo (no es una prueba)."""

    @classmethod
    def setUpClass(cls):
        extra = {"_evt": E, "_saldo": S, "print": lambda *a, **k: None}
        cls.a = F.cargar(["_ot_desinst_normalizar", "_ot_desinst_validar_previo"], extra=extra)
        cls.a["_ot_validar_tecnicos_una_empresa"] = lambda ids: None
        cls.a["_ot_validar_tope_ot_sin_contrato"] = lambda ids, extra=0: None
        cls.a["_feriados_cl"] = lambda *f: {}
        cls.a["_ot_desinst_remanente_saldo"] = lambda docs, ped=None, rut=None, ex=None: dict(cls.rem)

    rem = {"servicio": 0, "despacho": 0, "leido": True, "completo": True, "por_doc": []}

    def norm(self, **kw):
        base = {"lleva": True, "fecha": "2026-11-26", "hora_inicio": "09:00", "hora_fin": "13:00", "tecnico_user_ids": [3],
                "tecnico_lider_id": 3, "costo_proveedor": 0}
        base.update(kw)
        err, dd = self.a["_ot_desinst_normalizar"](base)
        self.assertIsNone(err)
        return dd

    def validar(self, dd, rem, m=None):
        self.__class__.rem = dict(rem)
        return self.a["_ot_desinst_validar_previo"](dd, m or madre(), [("NVV", "VD00010961")], None, None, None)



class TestA5_RandomCaidoNoSeAgendaAciegas(_AyudasValidarPrevio, unittest.TestCase):
    """MEDIO 5 / B6: con Random caído (o a medias) la desinstalación NO queda «cubierta» sin preguntar."""

    def test_random_sin_responder_bloquea(self):
        err, _ = self.validar(self.norm(), {"servicio": 0, "despacho": 0, "leido": False, "completo": False, "por_doc": []})
        self.assertEqual(err["codigo"], "LECTURA_INCOMPLETA")
        self.assertIn("Reintenta", err["mensaje"])

    def test_lectura_parcial_de_dos_documentos_bloquea(self):
        err, _ = self.validar(self.norm(), {"servicio": 40000, "despacho": 0, "leido": True, "completo": False, "por_doc": []})
        self.assertEqual(err["codigo"], "LECTURA_INCOMPLETA")

    def test_con_la_lectura_completa_sin_saldo_sigue_siendo_cubierta(self):
        dd = self.norm()
        err, _ = self.validar(dd, {"servicio": 0, "despacho": 0, "leido": True, "completo": True, "por_doc": []})
        self.assertIsNone(err)
        self.assertEqual(dd["cobro"], "cubierta")

    def test_el_saldo_remanente_marca_completo_solo_si_todo_se_leyo(self):
        def build(por_doc, lanza=False):
            def saldo_de(docs, excluir_vid=None, cliente_rut=None):
                if lanza:
                    raise RuntimeError("random caido")
                return {"por_doc": por_doc, "combinado": {"servicio": {"saldo": 100000}, "despacho": {"saldo": 0}}}
            return F.cargar(["_ot_desinst_remanente_saldo"], extra=dict(SIN_PRINT, _saldo=S, _ot_saldo_de_docs=saldo_de))["_ot_desinst_remanente_saldo"]
        docs = [("NVV", "VD00010961"), ("FCV", "11500")]
        r = build([{"leido": True}, {"leido": True}])(docs)
        self.assertTrue(r["completo"])
        self.assertEqual(r["servicio"], 100000)
        self.assertFalse(build([{"leido": True}, {"leido": False}])(docs)["completo"], "uno de dos sin leer")
        self.assertFalse(build([{"leido": True, "usos_incompletos": True}])(docs)["completo"], "el uso por otras OT quedó sin leer")
        self.assertFalse(build([], lanza=True)(docs)["completo"])
        self.assertTrue(build([])([])["completo"], "sin documentos no hay nada que leer")
        # una cotización no es un documento de Random: no puede dejar la lectura «incompleta» para siempre
        self.assertTrue(build([])([("COV", "55")])["completo"])

    def test_http_503_en_los_tres_caminos(self):
        self.assertIn('503 if e_v["codigo"] == "LECTURA_INCOMPLETA"', F.fuente_funcion("ot_api_desinstalacion_programar"))
        self.assertIn('http=(503 if _e_dd2["codigo"] == "LECTURA_INCOMPLETA"', F.fuente_funcion("_ot2_crear_core"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestValidarPrevioCobraYDomingo(_AyudasValidarPrevio, unittest.TestCase):
    def test_madre_que_no_se_cobra_no_admite_cubierta(self):
        garantia = madre(modalidad_cobro="garantia")
        err, _ = self.validar(self.norm(), {"servicio": 0, "despacho": 0, "leido": True, "completo": True, "por_doc": []}, garantia)
        self.assertEqual(err["codigo"], "DESINST_MADRE_SIN_COBRO")
        self.assertIn("no se cobra", err["mensaje"])
        # con saldo en la nota de venta puede elegir «la cobra la desinstalación» (camino normal con su propio documento)
        rem = {"servicio": 120000, "despacho": 0, "leido": True, "completo": True, "por_doc": []}
        err, _ = self.validar(self.norm(cobro="cobra", cobra_servicio=100000), rem, garantia)
        self.assertIsNone(err)
        # y si elige «cubierta» con saldo, también se rechaza
        err, _ = self.validar(self.norm(cobro="cubierta", argumento_cubierta="x" * 40), rem, garantia)
        self.assertEqual(err["codigo"], "DESINST_MADRE_SIN_COBRO")
        # una instalación en arriendo o con regalía igual
        for kw in ({"cobro_cero_motivo": "arriendo_leasing"}, {"cobro_cero_motivo": "regalia"}):
            err, _ = self.validar(self.norm(), {"servicio": 0, "despacho": 0, "leido": True, "completo": True, "por_doc": []}, madre(**kw))
            self.assertEqual(err["codigo"], "DESINST_MADRE_SIN_COBRO", kw)

    def test_por_definir_en_domingo_se_bloquea_en_el_servidor(self):
        dd = self.norm(lleva="por_definir", fecha="2026-11-29")      # domingo
        err, _ = self.validar(dd, self.rem)
        self.assertEqual(err["codigo"], "DOMINGO_BLOQUEADO")
        err, _ = self.validar(self.norm(lleva="por_definir", fecha="2026-11-30"), self.rem)
        self.assertIsNone(err)

    def test_equipos_ausentes_no_es_lista_vacia(self):
        n = self.a["_ot_desinst_normalizar"]
        self.assertIsNone(n({"lleva": True})[1]["equipos"])
        self.assertEqual(n({"lleva": True, "equipos": []})[1]["equipos"], [])
        self.assertEqual(n({"lleva": True, "equipos": "basura"})[0]["codigo"], "EQUIPO_INVALIDO")

    def test_manuales_fuera(self):
        n = self.a["_ot_desinst_normalizar"]
        self.assertEqual(n({"lleva": True})[1]["manuales_fuera"], [])
        self.assertEqual(n({"lleva": True, "manuales_fuera": [2, "0", 2, -1]})[1]["manuales_fuera"], [0, 2])
        self.assertEqual(n({"lleva": True, "manuales_fuera": ["x"]})[0]["codigo"], "MANUAL_INVALIDO")


class TestCrearHijaRespetaLoElegido(unittest.TestCase):
    def test_el_servidor_no_agrega_por_detras(self):
        src = F.fuente_funcion("_ot_crear_desinstalacion")
        self.assertIn('dd["equipos"] if dd.get("equipos") is not None', src)
        self.assertIn('"EQUIPOS_REQUERIDOS"', src)
        self.assertIn('set(dd.get("manuales_fuera") or [])', src)
        self.assertIn("for i, x in enumerate(her[\"manuales\"]) if i not in _fuera_m", src)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA6_AprobarRevalida(unittest.TestCase):
    """MEDIO 6: al aprobar una 'desinstalacion_cubierta' se vuelve a mirar todo con lo de HOY; y no se pide sin documento."""

    def amb(self, **kw):
        base = dict(SIN_PRINT, _evt=E, json=json, _ot_hijas_de=lambda vid: [], _ot_madre_tiene_doc_real=lambda vid: True,
                    _ot_madre_aut_incluia=lambda m: False, _evento_fila=lambda eid: None,
                    mysql_fetchone=lambda sql, p=(): {"id": 10, "rut": "76.000.000-0", "cliente_id": 5},
                    _ot_saldo_docs_de_ot=lambda vid: [("NVV", "VD1")], _validar_disponibilidad_visita=lambda *a, **k: {},
                    _ot_desinst_validar_previo=lambda dd, m, docs, ped, rut, ex, evento=None: (None, {}),
                    _ot_fila_basica=lambda vid: madre(), _ot_desinst_normalizar=lambda d: (None, dict(d, fecha="2026-11-26", hora_inicio="09:00",
                                                                                                    hora_fin="13:00", tecnico_lider_id=3)))
        base.update(kw)
        return F.cargar(["_ot_desinst_revalidar_al_aprobar"], extra=base)["_ot_desinst_revalidar_al_aprobar"]

    def sol(self, **pl):
        return {"visita_id": 10, "payload_json": json.dumps(pl)}

    def test_sin_bloque_solo_declara_la_fecha(self):
        self.assertIsNone(self.amb()(self.sol(prevista="2026-11-26")))

    def test_la_instalacion_ya_no_existe_o_ya_tiene_hija(self):
        self.assertEqual(self.amb(_ot_fila_basica=lambda vid: None)(self.sol())["codigo"], "OT_NO_EXISTE")
        r = self.amb(_ot_hijas_de=lambda vid: [hija()])(self.sol(desinstalacion={"fecha": "2026-11-26"}))
        self.assertEqual(r["codigo"], "DESINST_NO_PERMITIDA")

    def test_sin_documento_de_random(self):
        r = self.amb(_ot_madre_tiene_doc_real=lambda vid: False)(self.sol(desinstalacion={"fecha": "2026-11-26"}))
        self.assertEqual(r["codigo"], "DESINST_SIN_DOCUMENTO_MADRE")

    def test_corre_de_nuevo_la_validacion_previa(self):
        r = self.amb(_ot_desinst_validar_previo=lambda *a, **k: ({"codigo": "ZZ_SALDO_CONSUMIDO", "mensaje": "otra OT tomó el saldo"}, None))(
            self.sol(desinstalacion={"fecha": "2026-11-26"}))
        self.assertEqual(r["codigo"], "ZZ_SALDO_CONSUMIDO")

    def test_el_tecnico_se_ocupo(self):
        r = self.amb(_validar_disponibilidad_visita=lambda *a, **k: {"choque": {"tecnico_nombre": "Lenin"}})(
            self.sol(desinstalacion={"fecha": "2026-11-26"}))
        self.assertEqual(r["codigo"], "CHOQUE_HORARIO")
        self.assertIn("Lenin", r["mensaje"])

    def test_todo_sigue_valido(self):
        self.assertIsNone(self.amb()(self.sol(desinstalacion={"fecha": "2026-11-26"})))

    def test_se_usa_antes_de_marcar_aprobada_y_al_pedir_exige_documento(self):
        aprobar = F.fuente_funcion("ot_aut_api_aprobar")
        self.assertLess(aprobar.index("_ot_desinst_revalidar_al_aprobar(a)"), aprobar.index("conn = get_mysql()"))
        self.assertIn("La solicitud sigue pendiente.", aprobar)
        crear = F.fuente_funcion("ot_aut_api_crear")
        self.assertIn("DESINST_SIN_DOCUMENTO_MADRE", crear)
        self.assertIn("_ot_madre_aut_incluia(_mrow)", crear)
        # quien la pide puede editar ESA OT, y lo que se iba a agendar se valida al pedir con lo mismo que al aprobar
        i = crear.index("_ot_madre_aut_incluia(_mrow)")
        self.assertIn('_puede_ot_accion(visita_id, "metadata")', crear[i:i + 1600])
        self.assertIn("_ot_desinst_revalidar_al_aprobar({", crear[i:i + 2200])

    def test_programar_pide_el_documento_antes_que_la_autorizacion_y_no_lo_reemplaza(self):
        src = F.fuente_funcion("ot_api_desinstalacion_programar")
        self.assertLess(src.index("DESINST_SIN_DOCUMENTO_MADRE"), src.index("_ot_desinst_autorizar("))
        self.assertIn("_ot_madre_aut_incluia(madre)", src)
        self.assertNotIn("not (tiene_doc or aut_ok)", src)
        # y la puerta usa SOLO la autorización que incluía la desinstalación
        self.assertIn("_ot_madre_aut_incluia(madre)", F.fuente_funcion("_ot_cubierta_madre_check"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestA7_ReservaDeSaldoParaLaHija(unittest.TestCase):
    """BAJO 7: la instalación que cobró menos que su saldo porque lo dejó para su desinstalación no queda cobrando de menos en silencio."""

    class Bd:
        def __init__(self, clasif):
            self.clasif, self.updates, self.logs = clasif, [], []

        def fetchone(self, sql, params=()):
            return {"fin_clasif_json": json.dumps(self.clasif)} if self.clasif is not None else None

        def execute(self, sql, params=()):
            self.updates.append((sql, params))

    def amb(self, bd, fila=None):
        return F.cargar(["_ot_reserva_hija_vigente", "_ot_reserva_hija_liberar", "_ot_reserva_hija_si_se_cancelo"], extra=dict(
            SIN_PRINT, json=json, datetime=dt.datetime, _evt=E, _saldo=S, mysql_fetchone=bd.fetchone, mysql_execute=bd.execute,
            _mant_log=lambda *a, **k: bd.logs.append(a), current_username=lambda: "aaron",
            chile_fmt_filter=lambda v, fmt=None: "20/10/2026 10:00", _ot_fila_basica=lambda vid: fila))

    def test_guarda_la_marca_al_crear(self):
        self.assertIn("reserva_hija=bool(campos.get(\"reserva_hija\"))", F.fuente_funcion("_ot_fin_clasif_guardar"))

    def test_liberar_asienta_clasificacion_y_bitacora_una_sola_vez(self):
        bd = self.Bd({"codigo": "factura", "codigos": ["factura"], "otros": [], "reserva_hija": True})
        a = self.amb(bd)
        self.assertIsNotNone(a["_ot_reserva_hija_vigente"](10))
        self.assertTrue(a["_ot_reserva_hija_liberar"](10, "se canceló la desinstalación OT-2026-00351"))
        js = json.loads(bd.updates[0][1][0])
        self.assertIn("ajuste_manual", js["codigos"])
        self.assertEqual(js["reserva_hija_liberada"]["por"], "aaron")
        self.assertIn("OT-2026-00351", js["reserva_hija_liberada"]["motivo"])
        self.assertEqual(bd.logs[0][1], 10)
        self.assertEqual(bd.logs[0][2], "reserva_saldo_liberada")
        # ya liberada: no se vuelve a asentar
        bd2 = self.Bd(js)
        self.assertFalse(self.amb(bd2)["_ot_reserva_hija_liberar"](10, "otra vez"))
        self.assertEqual(bd2.updates, [])
        # una instalación que no reservó nada no se toca
        bd3 = self.Bd({"codigo": "factura"})
        self.assertFalse(self.amb(bd3)["_ot_reserva_hija_liberar"](10, "x"))
        self.assertFalse(self.amb(self.Bd(None))["_ot_reserva_hija_liberar"](10, "x"))

    def test_cancelar_la_hija_que_cobraba_su_linea_libera_la_reserva(self):
        bd = self.Bd({"codigo": "factura", "reserva_hija": True})
        fila = dict(hija(), zz_monto=80000, zz_envio_monto=None)
        self.assertTrue(self.amb(bd, fila)["_ot_reserva_hija_si_se_cancelo"](11))
        self.assertEqual(bd.logs[0][1], 10)
        # una hija cubierta no tomaba nada de la reserva
        bd2 = self.Bd({"codigo": "factura", "reserva_hija": True})
        self.assertFalse(self.amb(bd2, dict(hija(), zz_monto=None, zz_envio_monto=None))["_ot_reserva_hija_si_se_cancelo"](11))
        self.assertEqual(bd2.logs, [])
        # una OT que no es hija
        self.assertFalse(self.amb(bd2, madre())["_ot_reserva_hija_si_se_cancelo"](10))

    def test_quitar_la_prevista_pide_un_motivo_de_argumento(self):
        src = F.fuente_funcion("ot_api_desinstalacion_prevista")
        self.assertIn("_ot_reserva_hija_vigente(vid) is not None", src)
        self.assertIn("_evt.ARGUMENTO_MIN if _con_reserva else _evt.MOTIVO_MIN", src)
        self.assertIn("_ot_reserva_hija_liberar(vid,", src)
        self.assertIn("_ot_reserva_hija_si_se_cancelo(vid)", F.fuente_funcion("mant_visita_update"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestB_LogicaPura(unittest.TestCase):
    def test_b2_la_fecha_sugerida_nunca_queda_bloqueada(self):
        escenarios = []
        for fecha_ini, fecha_fin, hfin in ((D(2026, 10, 26), None, "13:00"), (D(2026, 10, 26), D(2026, 10, 27), "18:00"),
                                           (D(2026, 10, 30), None, "17:00"), (D(2026, 10, 23), None, "20:00")):
            escenarios.append(madre(fecha_programada=fecha_ini, fecha_fin=fecha_fin, hora_fin=hfin))
        fer = {"2026-11-02": "feriado de prueba"}
        for m in escenarios:
            p = E.proponer_fecha_desinstalacion(m, fer, D(2026, 10, 20))
            r = E.evaluar_fecha_desinstalacion(p["fecha"], "09:00", "13:00", m, fer)
            self.assertTrue(r["ok"], (m, p, r["bloqueo"]))

    def test_b2_casos_concretos(self):
        # el siguiente hábil después de que termina la instalación (el caso del arnés: antes proponía el mismo día)
        p = E.proponer_fecha_desinstalacion(madre(fecha_programada=D(2026, 10, 26), fecha_fin=None, hora_fin="13:00"), {})
        self.assertEqual(p["fecha"], D(2026, 10, 27))
        self.assertIn("siguiente a la instalación", p["motivo"])
        # una instalación que termina un viernes: lunes; con el lunes feriado: martes
        m = madre(fecha_programada=D(2026, 10, 30), fecha_fin=None)
        self.assertEqual(E.proponer_fecha_desinstalacion(m, {})["fecha"], D(2026, 11, 2))
        self.assertEqual(E.proponer_fecha_desinstalacion(m, {"2026-11-02": "x"})["fecha"], D(2026, 11, 3))
        # instalación ya hecha: manda su fecha real
        real = madre(fecha_realizada=D(2026, 10, 22), fecha_programada=D(2026, 10, 26))
        self.assertEqual(E.proponer_fecha_desinstalacion(real, {})["fecha"], D(2026, 10, 23))
        # nunca en el pasado
        self.assertEqual(E.proponer_fecha_desinstalacion(madre(), {}, hoy=D(2026, 12, 1))["fecha"], D(2026, 12, 1))
        self.assertEqual(E.proponer_fecha_desinstalacion(None, {}), {"fecha": None, "motivo": None})

    def test_b7_cancelar_en_cascada_no_toca_una_hija_ya_trabajada(self):
        self.assertTrue(E.sin_empezar("pendiente_info"))
        self.assertFalse(E.sin_empezar("pendiente_info", {"firma_tecnico_at": dt.datetime(2026, 11, 26, 15)}))
        self.assertFalse(E.sin_empezar("programada", {"hora_real_inicio": dt.datetime(2026, 11, 26, 9)}))
        self.assertFalse(E.sin_empezar("programada", {"fecha_realizada": D(2026, 11, 26)}))
        self.assertTrue(E.sin_empezar("programada", {"firma_tecnico_at": None}))
        self.assertFalse(E.sin_empezar("en_curso"))
        acc, err = E.guardia_cancelar_madre([hija(estado="pendiente_info", firma_tecnico_at=dt.datetime(2026, 11, 26, 15))], True)
        self.assertIsNone(acc)
        self.assertEqual(err["codigo"], "DESINST_EN_CURSO")
        self.assertEqual(E.guardia_cancelar_madre([hija(estado="pendiente_info")], True), ("con_hija", None))
        # y deja de contarse como «vencida»
        est = E.estado_desinstalacion(madre(), [hija(estado="pendiente_info", firma_tecnico_at=dt.datetime(2026, 11, 26, 15))], D(2026, 12, 1))
        self.assertEqual(est["codigo"], "agendada")
        est2 = E.estado_desinstalacion(madre(), [hija(estado="programada")], D(2026, 12, 1))
        self.assertEqual(est2["codigo"], "vencida")

    def test_b8_la_hora_en_el_mensaje(self):
        r = E.evaluar_fecha_desinstalacion("2026-10-27", "09:00", "10:00", madre(fecha_programada=D(2026, 10, 27), fecha_fin=None, hora_fin="13:00"), {})
        self.assertFalse(r["ok"])
        self.assertIn("(13:00)", r["bloqueo"]["mensaje"])
        self.assertNotIn("()", r["bloqueo"]["mensaje"])
        r2 = E.evaluar_fecha_desinstalacion("2026-10-27", None, None, madre(fecha_programada=D(2026, 10, 27), fecha_fin=None, hora_fin="13:00"), {})
        self.assertTrue(r2["ok"])
        self.assertIn("(13:00)", r2["avisos"][0])
        self.assertEqual(E.min_txt(75), "01:15")
        self.assertEqual(E.min_txt(None), "")

    def test_baja5_reagendar_la_instalacion_mira_las_horas(self):
        h = [hija(fecha_programada=D(2026, 10, 28), hora_inicio="09:00")]
        self.assertIsNone(E.guardia_reagendar_madre("2026-10-27", None, h, "18:00"))               # un día antes: bien
        self.assertEqual(E.guardia_reagendar_madre("2026-10-29", None, h, "10:00")["codigo"], "REAGENDAR_DESINSTALACION_PRIMERO")
        g = E.guardia_reagendar_madre("2026-10-28", None, h, "13:00")                              # el mismo día, termina después de que empieza
        self.assertEqual(g["codigo"], "REAGENDAR_DESINSTALACION_PRIMERO")
        self.assertIn("09:00", g["mensaje"])
        self.assertIsNone(E.guardia_reagendar_madre("2026-10-28", None, h, "08:30"))               # termina antes de que empiece
        self.assertIsNone(E.guardia_reagendar_madre("2026-10-28", None, h, None))                  # sin hora no se inventa
        self.assertIsNone(E.guardia_reagendar_madre("2026-10-28", None, [hija(fecha_programada=D(2026, 10, 28), hora_inicio=None)], "13:00"))

    def test_baja4_desinstalacion_vencida_con_la_instalacion_ya_hecha_avisa(self):
        m = madre(estado="cerrada")
        av = E.aviso_barrido(m, [hija(fecha_programada=D(2026, 11, 26))], D(2026, 12, 1))
        self.assertEqual(av["codigo"], "desinst_vencida")
        self.assertEqual(av["nivel"], "rojo")
        self.assertIn("OT-2026-00351", av["texto"])
        # a tiempo, hecha, o ya empezada: no avisa
        self.assertIsNone(E.aviso_barrido(m, [hija(fecha_programada=D(2026, 12, 15))], D(2026, 12, 1)))
        self.assertIsNone(E.aviso_barrido(m, [hija(fecha_programada=D(2026, 11, 26), estado="cerrada")], D(2026, 12, 1)))
        self.assertIsNone(E.aviso_barrido(m, [hija(fecha_programada=D(2026, 11, 26), estado="en_curso")], D(2026, 12, 1)))
        # lo de siempre: la hija toca y la instalación no se ejecutó
        av2 = E.aviso_barrido(madre(), [hija(fecha_programada=D(2026, 11, 26))], D(2026, 11, 26))
        self.assertEqual(av2["codigo"], "hija_sin_madre_ejecutada")

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestB_Servidor(unittest.TestCase):
    def test_b3_la_aprobacion_devuelve_lo_que_paso_con_la_desinstalacion(self):
        aprobar = F.fuente_funcion("ot_aut_api_aprobar")
        self.assertIn('"desinstalacion": cuerpo.get("desinstalacion")', aprobar)

    def test_b4_una_falla_de_la_desinstalacion_no_impide_crear_la_instalacion_aprobada(self):
        n = F.fuente_funcion("_ot2_crear_core")
        self.assertIn("if _e_dd2 and autorizacion:", n)
        self.assertIn("_dd_falla = {", n)
        self.assertIn("if _ch_dd and autorizacion:", n)
        # sin autorización (creación normal) el error sigue frenando TODO antes de crear nada
        i = n.index("elif _e_dd2:")
        self.assertIn("return _ot2_err(_e_dd2", n[i:i + 200])
        # la respuesta trae el error y se le avisa a quien la pidió
        self.assertIn("if _dd_falla:", n)
        self.assertIn('"ok": False, "codigo": _dd_falla["codigo"]', n)
        self.assertIn("_mant_notificar(int(_uid_sol)", n)

    def test_b7_la_cascada_no_cancela_una_hija_ya_firmada(self):
        put = F.fuente_funcion("mant_visita_update")
        self.assertIn("AND firma_tecnico_at IS NULL AND hora_real_inicio IS NULL AND fecha_realizada IS NULL", put)

    def test_las_hijas_traen_si_ya_se_trabajaron(self):
        for fn in ("_ot_hijas_de", "_ot_hijas_vigentes_lote"):
            src = F.fuente_funcion(fn)
            self.assertIn("firma_tecnico_at", src, fn)
            self.assertIn("hora_real_inicio", src, fn)

    def test_baja3_una_ot_cancelada_no_muestra_chips_de_relacion(self):
        src = F.fuente_funcion("_ot2_panel_evento_chips")
        i = src.index("if not _evt.vigente(b.get(\"estado\")):")
        self.assertLess(i, src.index('f["rel"] = {"tipo": "hija"'))
        self.assertLess(i, src.index('est = _evt.estado_desinstalacion(b, hijas, hoy)'))

    def test_el_contexto_de_la_ficha_y_el_previo_dicen_si_la_instalacion_se_cobra(self):
        self.assertIn('out["madre_cobra"] = _evt.madre_cobra(f)', F.fuente_funcion("_ot_desinst_contexto"))
        self.assertIn('out["madre_cobra"] = _evt.madre_cobra(f)', F.fuente_funcion("ot_api_desinstalacion_info"))
        previo = F.fuente_funcion("ot_api_desinstalacion_previo")
        self.assertIn('"madre_cobra": _evt.madre_cobra(madre_virtual)', previo)
        self.assertIn('d.get("madre_sin_cobro")', previo)
        self.assertIn('"completo": rem.get("completo", True)', previo)

    def test_el_asistente_pasa_la_cobertura_de_la_instalacion_a_la_validacion(self):
        n = F.fuente_funcion("_ot2_crear_core")
        self.assertIn('"modalidad_cobro": _fin_campos.get("modalidad_cobro")', n)
        self.assertIn("_cero_virtual", n)
        self.assertIn("(autorizacion or {}).get(\"motivo\") in _OT_COBRO_CERO_MOTIVOS", n)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestFrente(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.aut = leer("static", "ot_autorizaciones.js")
        cls.cancelar = leer("static", "ot_cancelar.js")
        cls.ficha = leer("static", "ot_desinst.js")
        cls.css = leer("static", "ot_evento.css")
        cls.modal = leer("templates", "ot2", "_modal_crear.html")
        cls.base = leer("templates", "base.html")
        cls.cal = leer("templates", "mantenciones", "calendario.html")
        cls.ot2form = leer("static", "ot2_form.js")
        cls.tkficha = leer("static", "tickets_ficha.js")

    def test_b3_la_bandeja_muestra_lo_que_paso_con_la_desinstalacion(self):
        self.assertIn("r.j.desinstalacion", self.aut)
        self.assertIn("d.ok === false", self.aut)
        self.assertIn("ilusAlert", self.aut)

    def test_b5_cancelar_una_instalacion_con_desinstalacion_tiene_salida(self):
        c = self.cancelar
        self.assertIn("OT_CON_DESINSTALACION", c)
        self.assertIn("incluir_desinstalacion: 1", c)
        self.assertIn("ilusConfirm", c)
        for codigo in ("REAGENDAR_DESINSTALACION_PRIMERO", "DESINST_EN_CURSO"):
            self.assertIn(codigo, c)
        self.assertNotRegex(c, r"(?<![\w.$])(alert|confirm|prompt)\(")
        self.assertIn("ot_cancelar.js", self.base)
        for nombre, fuente in (("ot2_form.js", self.ot2form), ("tickets_ficha.js", self.tkficha), ("calendario.html", self.cal)):
            self.assertIn("window.OTCancelar.enviar", fuente, nombre)
            self.assertIn("avisarError", fuente, nombre)

    def test_b5_el_ayudante_se_ejecuta_en_node(self):
        import shutil, subprocess, tempfile
        node = shutil.which("node")
        if not node:
            self.skipTest("node no está disponible")
        guion = r"""
const fs = require('fs');
const llamadas = [];
global.window = global;
let respuestas = [
  {status: 409, body: {ok: false, error_codigo: 'OT_CON_DESINSTALACION', puede_incluir_desinstalacion: true, hija_numero: 'OT-2026-00351', error: 'x'}},
  {status: 200, body: {ok: true}}
];
global.fetch = (url, init) => { llamadas.push(JSON.parse(init.body)); const r = respuestas.shift();
  return Promise.resolve({ok: r.status < 400, status: r.status, json: () => Promise.resolve(r.body)}); };
let pregunto = null;
global.ilusConfirm = (o) => { pregunto = o; return Promise.resolve(process.argv[2] === 'si'); };
global.ilusAlert = (o) => { global.alerta = o; return Promise.resolve(true); };
eval(fs.readFileSync(process.argv[3], 'utf8'));
window.OTCancelar.enviar(10, {estado: 'cancelada'}).then(r => {
  console.log(JSON.stringify({ok: r.ok, cancelado: r.cancelado, llamadas: llamadas, titulo: pregunto && pregunto.title, ok_label: pregunto && pregunto.okLabel}));
  const hay = window.OTCancelar.avisarError({error_codigo: 'REAGENDAR_DESINSTALACION_PRIMERO', hija_id: 11, hija_numero: 'OT-2026-00351', error: 'Reagenda primero'});
  console.log(JSON.stringify({aviso: hay, enlace: global.alerta && global.alerta.sub, html: global.alerta && global.alerta.subHtml, otro: window.OTCancelar.avisarError({error_codigo: 'OTRA'})}));
});
"""
        with tempfile.TemporaryDirectory() as td:
            js = os.path.join(td, "t.js")
            with open(js, "w", encoding="utf-8") as fh:
                fh.write(guion)
            ruta = os.path.join(F.RAIZ, "static", "ot_cancelar.js")
            sal = subprocess.run([node, js, "si", ruta], capture_output=True, text=True, timeout=60).stdout.strip().split("\n")
            r1, r2 = json.loads(sal[0]), json.loads(sal[1])
            self.assertTrue(r1["ok"])
            self.assertEqual(len(r1["llamadas"]), 2)
            self.assertEqual(r1["llamadas"][0], {"estado": "cancelada"})
            self.assertEqual(r1["llamadas"][1], {"estado": "cancelada", "incluir_desinstalacion": 1})
            self.assertEqual(r1["ok_label"], "Cancelar las dos")
            self.assertTrue(r2["aviso"])
            self.assertIn("/ot/11", r2["enlace"])
            self.assertTrue(r2["html"])
            self.assertFalse(r2["otro"])
            # si la persona prefiere volver, no se reenvía nada
            sal = subprocess.run([node, js, "no", ruta], capture_output=True, text=True, timeout=60).stdout.strip().split("\n")
            r3 = json.loads(sal[0])
            self.assertEqual((r3["ok"], r3["cancelado"], len(r3["llamadas"])), (False, True, 1))

    def test_a1_a5_el_asistente_y_la_ficha_muestran_lo_que_el_servidor_exige(self):
        m = self.modal
        self.assertIn("madre_sin_cobro: !!S.fin_garantia", m)
        self.assertIn("var madreCobra = p.madre_cobra !== false;", m)
        self.assertIn("(madreCobra ? '<button type=\"button\" class=\"o2m-di-op'+(d.cobro==='cubierta'", m)
        self.assertNotIn("el servidor lo vuelve a revisar al crear.</div>' : '')+'</div>' : '';", m)
        f = self.ficha
        self.assertIn("function madreCobra() { return S.info.madre_cobra !== false; }", f)
        self.assertIn("function lecturaIncompleta() { return remanente().completo === false; }", f)
        self.assertIn("if (lecturaIncompleta()) return false;", f)
        self.assertIn("data-reintentar", f)
        self.assertNotIn("el servidor lo vuelve a revisar al crear", f)

    def test_baja1_la_casilla_del_equipo_del_asistente_es_un_label(self):
        i = self.modal.index("function _diRegEq(el){")
        j = self.modal.index("function _diRegNota(el){")
        trozo = self.modal[i:j]
        self.assertIn("return '<label class=\"o2m-di-eq\"><input type=\"checkbox\"", trozo)
        self.assertNotIn("return '<div class=\"o2m-di-eq\">", trozo)

    def test_baja7_los_productos_manuales_se_pueden_dejar_fuera(self):
        f = self.ficha
        self.assertIn("data-man=", f)
        self.assertIn("data-mantodo", f)                # REGLA #14: marcar/desmarcar todo
        self.assertIn("o.manuales_fuera = mf", f)
        self.assertIn("manFuera: {}", f)

    def test_baja2_enlaces_de_44_px_en_celular(self):
        self.assertRegex(self.css, r"max-width:640px\)\{\.ev-link\{display:inline-flex;align-items:center;min-height:44px\}\}")

    def test_regla_1_ninguna_pantalla_nueva_usa_popups_nativos(self):
        for nombre, fuente in (("ot_cancelar", self.cancelar), ("ot_desinst", self.ficha), ("ot_autorizaciones", self.aut)):
            self.assertNotRegex(fuente, r"(?<![\w.$])(alert|confirm|prompt)\(", nombre)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""🧭 INTENCIONES DEL COSTEO (REGLA #47) — Daniel, 2026-10-25: «agregar los motivos, pero solamente para ser administrador: garantía, regalía, arriendo,
leasing, evento, marketing, el precio lo asume comercial… dinámico para la toma de decisiones en el mes» y «todo cobro… sea CERRADO con FCV o BLV; de lo
contrario bloquea la OT solicitando autorización, ya que puede ser arriendo, por descuadre de precio o pérdida, puede ser costo asumido por marketing…
¿Quién lo asume? ¿Por qué? Autorizaron. Toda esa trazabilidad la necesito».

Cada autorización lleva su «¿Por qué?» clasificado (catálogo administrable `ot_intenciones`), el argumento de quien pide y un «mensaje del sistema»; la
familia contable del $0 (garantía/regalía/arriendo_leasing) NO cambia. Todo con datos y base de mentira: nunca contra producción ni con correos reales.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest
from datetime import date

import ot_intenciones as I
import ot_saldo_servicio as SALDO
from tests._fuente_app import cargar, decoradores, fuente_funcion, texto

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


CAT = I.catalogo_semilla()


def _cat(**cambios):
    """El catálogo de la semilla con ajustes por clave: _cat(evento={'activa': False})."""
    out = []
    for f in CAT:
        f = dict(f)
        f.update(cambios.get(f["clave"], {}))
        out.append(f)
    return out


# ───────────────────────── lógica pura ─────────────────────────
class TestLaSemilla(unittest.TestCase):
    def test_trae_las_tres_familias_del_costeo_y_lo_que_pidio_daniel(self):
        claves = {f["clave"] for f in CAT}
        for k in ("garantia", "regalia", "arriendo_leasing", "evento", "marketing", "comercial_asume", "descuadre_precio", "mal_cobro",
                  "perdida_asumida", "documento_en_tramite", "otro"):
            self.assertIn(k, claves)
        self.assertEqual(len(CAT), 11)

    def test_garantia_se_llama_garantia_y_su_familia_es_ella_misma(self):
        g = I.por_clave(CAT)["garantia"]
        self.assertEqual((g["nombre"], g["familia_cero"], g["sistema"]), ("Garantía", "garantia", True))
        for k, fam in (("regalia", "regalia"), ("arriendo_leasing", "arriendo_leasing")):
            self.assertEqual(I.por_clave(CAT)[k]["familia_cero"], fam)

    def test_los_costos_asumidos_por_otra_area_son_una_regalia_contable_y_dicen_quien_asume(self):
        c = I.por_clave(CAT)
        self.assertEqual((c["marketing"]["familia_cero"], c["marketing"]["asume_centro"]), ("regalia", "marketing"))
        self.assertEqual((c["comercial_asume"]["familia_cero"], c["comercial_asume"]["asume_centro"]), ("regalia", "comercial"))
        self.assertEqual(c["comercial_asume"]["nombre"], "Costo asumido en el precio de Comercial")

    def test_otro_motivo_aplica_a_todo_y_no_se_puede_desactivar(self):
        o = I.por_clave(CAT)["otro"]
        self.assertEqual(set(o["aplica_a"]), set(I.USOS))
        self.assertFalse(I.puede_desactivar(o)[0])
        self.assertTrue(I.puede_desactivar(I.por_clave(CAT)["evento"])[0])

    def test_nada_arranca_solo_administrador(self):
        self.assertFalse(any(f["solo_admin"] for f in CAT))

    def test_el_insert_ignore_trae_la_lista_como_texto(self):
        f = I.semilla_filas()[0]
        self.assertIsInstance(f["aplica_a"], str)
        self.assertIn("cobertura", f["aplica_a"])
        self.assertEqual(f["solo_admin"], 0)


class TestUsos(unittest.TestCase):
    def test_cada_tipo_de_autorizacion_se_clasifica_por_su_situacion(self):
        self.assertEqual(I.uso_de("cobro_cero"), "cobertura")
        for t in ("cerrar_sin_documento", "cerrar_con_perdida", "exceder_saldo", "crear_sin_documento", "documento_observado"):
            self.assertEqual(I.uso_de(t), t)
        self.assertIsNone(I.uso_de("desinstalacion_cubierta"))
        self.assertIsNone(I.uso_de("inventado"))

    def test_las_situaciones_de_daniel_estan_cubiertas(self):
        # arriendo, descuadre de precio, pérdida y costo asumido por marketing, al CERRAR sin factura/boleta o con pérdida
        c = I.por_clave(CAT)
        self.assertIn("cerrar_sin_documento", c["arriendo_leasing"]["aplica_a"])
        self.assertIn("exceder_saldo", c["descuadre_precio"]["aplica_a"])
        self.assertIn("cerrar_con_perdida", c["perdida_asumida"]["aplica_a"])
        self.assertIn("cerrar_con_perdida", c["marketing"]["aplica_a"])
        self.assertIn("cerrar_sin_documento", c["marketing"]["aplica_a"])


class TestValidar(unittest.TestCase):
    def _v(self, clave, uso="cerrar_sin_documento", admin=False, requerida=True, cat=None):
        return I.validar(clave, cat if cat is not None else CAT, uso, admin, requerida)

    def test_sin_clasificar_es_un_error_claro(self):
        err, f = self._v("")
        self.assertEqual(err["error_codigo"], "INTENCION_REQUERIDA")
        self.assertIsNone(f)
        self.assertEqual(self._v("", requerida=False), (None, None))

    def test_una_que_no_existe_no_pasa(self):
        self.assertEqual(self._v("inventada")[0]["error_codigo"], "INTENCION_INVALIDA")

    def test_una_desactivada_no_se_puede_elegir(self):
        err, _ = self._v("evento", cat=_cat(evento={"activa": False}))
        self.assertEqual(err["error_codigo"], "INTENCION_INACTIVA")

    def test_una_que_no_aplica_a_esa_situacion_no_pasa(self):
        err, _ = self._v("mal_cobro", uso="cobertura")
        self.assertEqual(err["error_codigo"], "INTENCION_NO_APLICA")
        self.assertIn("Mal cobro en los servicios", err["error"])

    def test_solo_administrador_solo_la_elige_un_administrador(self):
        cat = _cat(marketing={"solo_admin": True})
        err, _ = self._v("marketing", cat=cat, admin=False)
        self.assertEqual(err["error_codigo"], "INTENCION_SOLO_ADMIN")
        self.assertEqual(self._v("marketing", cat=cat, admin=True)[0], None)

    def test_la_valida_ofrecida_pasa_y_devuelve_su_fila(self):
        err, f = self._v("marketing", uso="cerrar_con_perdida")
        self.assertIsNone(err)
        self.assertEqual(f["clave"], "marketing")
        self.assertEqual(self._v("  MARKETING  ", uso="cerrar_con_perdida")[1]["clave"], "marketing")     # tolera mayúsculas y espacios


class TestLoQueSeOfrece(unittest.TestCase):
    def test_visibles_filtra_por_situacion_estado_y_rol(self):
        cat = _cat(evento={"activa": False}, comercial_asume={"solo_admin": True})
        pers = {f["clave"] for f in I.visibles(cat, "cobertura", False)}
        adm = {f["clave"] for f in I.visibles(cat, "cobertura", True)}
        self.assertIn("garantia", pers)
        self.assertNotIn("evento", pers | adm)                      # desactivada: nadie la ve
        self.assertNotIn("comercial_asume", pers)                  # solo administrador
        self.assertIn("comercial_asume", adm)
        self.assertNotIn("mal_cobro", pers)                         # no aplica a un $0
        # una selección anterior se repinta aunque ya no se pueda elegir
        self.assertIn("evento", {f["clave"] for f in I.visibles(cat, "cobertura", False, incluir="evento")})

    def test_familia_de_un_cero(self):
        self.assertEqual(I.familia_de("garantia"), "garantia")
        self.assertEqual(I.familia_de("evento", CAT), "regalia")
        self.assertIsNone(I.familia_de("mal_cobro", CAT))
        self.assertIsNone(I.familia_de("nada", CAT))


class TestSugerencia(unittest.TestCase):
    def test_marketing_sugiere_marketing_con_su_origen(self):
        s = I.sugerir(CAT, "cobertura", "marketing", "instalacion", False)
        self.assertEqual(s["clave"], "marketing")
        self.assertEqual(s["origen"], "Sugerida según su centro de costo (Marketing)")

    def test_comercial_sugiere_costo_asumido_en_el_precio(self):
        self.assertEqual(I.sugerir(CAT, "cobertura", "comercial", "instalacion", False)["clave"], "comercial_asume")
        self.assertEqual(I.sugerir(CAT, "cerrar_con_perdida", "comercial", None, False)["clave"], "comercial_asume")

    def test_servicio_tecnico_sugiere_garantia_solo_si_el_trabajo_es_de_servicio(self):
        self.assertEqual(I.sugerir(CAT, "cobertura", "sstt", "mantencion", False)["clave"], "garantia")
        self.assertIsNone(I.sugerir(CAT, "cobertura", "sstt", "instalacion", False))

    def test_sin_centro_o_sin_regla_no_inventa(self):
        self.assertIsNone(I.sugerir(CAT, "cobertura", None, "mantencion", False))
        self.assertIsNone(I.sugerir(CAT, "cobertura", "logistica", "mantencion", False))

    def test_solo_sugiere_lo_que_la_persona_podria_elegir(self):
        cat = _cat(marketing={"solo_admin": True})
        self.assertEqual(I.sugerir(cat, "cobertura", "marketing", None, False)["clave"], "evento")     # la segunda preferencia
        self.assertEqual(I.sugerir(cat, "cobertura", "marketing", None, True)["clave"], "marketing")
        cat2 = _cat(marketing={"activa": False}, evento={"activa": False})
        self.assertIsNone(I.sugerir(cat2, "cobertura", "marketing", None, True))
        # para «cobrar distinto del documento» (no aplica a evento/garantía) no hay sugerencia por centro de costo sstt
        self.assertIsNone(I.sugerir(CAT, "exceder_saldo", "sstt", "mantencion", False))


class TestGestion(unittest.TestCase):
    CENTROS = {"sstt", "logistica", "comercial", "marketing"}

    def _e(self, datos, existente=None, ocupadas=None):
        return I.validar_edicion(datos, existente, ocupadas or {f["clave"] for f in CAT}, self.CENTROS)

    def test_crea_con_clave_a_partir_del_nombre(self):
        err, l = self._e({"nombre": "Campaña de Verano", "aplica_a": ["cerrar_con_perdida"], "asume_centro": "marketing"})
        self.assertIsNone(err)
        self.assertEqual((l["clave"], l["asume_centro"], l["familia_cero"]), ("campana_de_verano", "marketing", None))

    def test_un_cero_sin_familia_cae_en_regalia_nunca_en_garantia(self):
        err, l = self._e({"nombre": "Muestra a gerencia", "aplica_a": ["cobertura"]})
        self.assertIsNone(err)
        self.assertEqual(l["familia_cero"], "regalia")

    def test_validaciones(self):
        self.assertEqual(self._e({"nombre": "ab", "aplica_a": ["cobertura"]})[0]["error_codigo"], "NOMBRE_CORTO")
        self.assertEqual(self._e({"nombre": "Algo nuevo", "aplica_a": []})[0]["error_codigo"], "USO_REQUERIDO")
        self.assertEqual(self._e({"nombre": "Algo nuevo", "aplica_a": ["inventado"]})[0]["error_codigo"], "USO_REQUERIDO")
        self.assertEqual(self._e({"nombre": "Algo nuevo", "aplica_a": ["cobertura"], "familia_cero": "otra"})[0]["error_codigo"], "FAMILIA_INVALIDA")
        self.assertEqual(self._e({"nombre": "Algo nuevo", "aplica_a": ["exceder_saldo"], "familia_cero": "regalia"})[0]["error_codigo"], "FAMILIA_SIN_COBERTURA")
        self.assertEqual(self._e({"nombre": "Algo nuevo", "aplica_a": ["cobertura"], "asume_centro": "luna"})[0]["error_codigo"], "CENTRO_INVALIDO")
        self.assertEqual(self._e({"nombre": "Evento", "aplica_a": ["cobertura"]})[0]["error_codigo"], "CLAVE_REPETIDA")

    def test_editar_no_cambia_la_clave_y_las_del_sistema_conservan_su_familia(self):
        g = I.por_clave(CAT)["garantia"]
        err, l = self._e({"nombre": "Garantía del proveedor", "aplica_a": ["cobertura", "cerrar_con_perdida"], "familia_cero": "regalia",
                          "clave": "otra_cosa"}, existente=g)
        self.assertIsNone(err)
        self.assertEqual((l["clave"], l["familia_cero"]), ("garantia", "garantia"))

    def test_las_tres_familias_siguen_siendo_un_cero(self):
        g = I.por_clave(CAT)["garantia"]
        self.assertEqual(self._e({"nombre": "Garantía", "aplica_a": ["cerrar_con_perdida"]}, existente=g)[0]["error_codigo"], "SISTEMA_COBERTURA")

    def test_otro_motivo_sigue_aplicando_a_todo(self):
        o = I.por_clave(CAT)["otro"]
        err, l = self._e({"nombre": "Otro motivo", "aplica_a": ["cobertura"]}, existente=o)
        self.assertIsNone(err)
        self.assertEqual(set(l["aplica_a"]), set(I.USOS))

    def test_slug(self):
        self.assertEqual(I.slug("  ¡Costo asumido por Gerencia! "), "costo_asumido_por_gerencia")
        self.assertTrue(I.slug("2026 promo").startswith("i_"))


class TestMensajeDelSistema(unittest.TestCase):
    def test_pierde(self):
        m = I.mensaje_sistema("cerrar_con_perdida", {"numero_ot": "OT-2026-00300", "perdida": 77899, "cobre": 302101, "me_cobraron": 380000})
        self.assertIn("pérdida de $77.899", m)
        self.assertIn("se cobró $302.101", m)
        self.assertIn("proveedor cobró $380.000", m)
        self.assertIn("OT-2026-00300", m)

    def test_saldo(self):
        m = I.mensaje_sistema("exceder_saldo", {"saldo_pedido": {"servicio": 120000}})
        self.assertIn("servicio $120.000", m)

    def test_sin_documento(self):
        self.assertIn("no tiene una factura ni boleta", I.mensaje_sistema("cerrar_sin_documento", {"numero_ot": "OT-1"}))
        self.assertIn("sin un documento de Random", I.mensaje_sistema("crear_sin_documento"))

    def test_documento_observado_trae_las_observaciones(self):
        m = I.mensaje_sistema("documento_observado", {"observaciones": ["Emitida hace 100 días.", "Sin saldo en Random."]})
        self.assertIn("Emitida hace 100 días. Sin saldo en Random.", m)

    def test_cobertura_dice_la_familia(self):
        self.assertIn("como Regalía", I.mensaje_sistema("cobro_cero", {"familia": "regalia"}))

    def test_un_tipo_que_no_se_clasifica_no_inventa_nada_y_nunca_pasa_de_600(self):
        self.assertEqual(I.mensaje_sistema("desinstalacion_cubierta"), "")
        self.assertLessEqual(len(I.mensaje_sistema("documento_observado", {"observaciones": ["x" * 2000]})), 600)

    def test_observaciones_txt(self):
        obs = [{"observaciones": [{"texto": "A"}, {"texto": "B"}]}, {"texto": "A"}, "C", " "]
        self.assertEqual(I.observaciones_txt(obs), ["A", "B", "C"])


class TestPorMes(unittest.TestCase):
    def test_agrupa_por_mes_intencion_y_centro_hora_chile(self):
        filas = [{"fecha": date(2026, 10, 5), "intencion": "Garantía", "centro": "Servicio Técnico", "nos": 100000, "val": 150000},
                 {"fecha": date(2026, 10, 20), "intencion": "Garantía", "centro": "Servicio Técnico", "nos": 50000, "val": None},
                 {"fecha": "2026-09-30", "intencion": "Costo asumido por Marketing", "centro": "Marketing", "nos": None, "val": 0},
                 {"fecha": None, "intencion": None, "centro": None, "nos": 10, "val": 0}]
        r = I.resumen_por_mes(filas)
        por = {(x["mes"], x["intencion"], x["centro"]): x for x in r}
        g = por[("2026-10", "Garantía", "Servicio Técnico")]
        self.assertEqual((g["n"], g["nos"], g["val"], g["sin_costo"]), (2, 150000.0, 150000.0, 0))
        m = por[("2026-09", "Costo asumido por Marketing", "Marketing")]
        self.assertEqual((m["n"], m["nos"], m["sin_costo"]), (1, 0.0, 1))            # no se inventa un $0: se cuenta como «sin costo»
        s = por[("Sin fecha", "Sin clasificar", "(sin centro de costo)")]
        self.assertEqual(s["n"], 1)
        self.assertEqual([x["mes"] for x in r], ["2026-09", "2026-10", "Sin fecha"])   # en orden de mes; lo sin fecha al final


# ───────────────────────── comportamiento con base de mentira ─────────────────────────
class TestElArranque(unittest.TestCase):
    def _env(self, existentes=(), falla_tabla=False):
        self.ddl, self.inserts = [], []

        def ejecutar(sql, params=None):
            s = " ".join(sql.split())
            if s.startswith("INSERT"):
                self.inserts.append((s, params))
            else:
                self.ddl.append(s)
                if falla_tabla and s.startswith("CREATE"):
                    raise RuntimeError("sin permisos")
        return cargar(funciones=("_ensure_ot_intenciones",),
                      extra={"_oint": I, "mysql_execute": ejecutar, "mysql_fetchall": lambda sql, p=None: [{"clave": k} for k in existentes],
                             "print": lambda *a, **k: None})["_ensure_ot_intenciones"]

    def test_crea_la_tabla_las_dos_columnas_y_la_siembra(self):
        self._env()()
        self.assertTrue(any(s.startswith("CREATE TABLE IF NOT EXISTS ot_intenciones") for s in self.ddl))
        self.assertIn("ALTER TABLE mant_visitas ADD COLUMN intencion VARCHAR(40) NULL", self.ddl)
        self.assertIn("ALTER TABLE mant_ot_autorizaciones ADD COLUMN intencion VARCHAR(40) NULL", self.ddl)
        self.assertIn("ALTER TABLE mant_ot_autorizaciones ADD COLUMN mensaje_sistema VARCHAR(600) NULL", self.ddl)
        self.assertEqual(len(self.inserts), 11)
        self.assertTrue(all("INSERT IGNORE" in s for s, _ in self.inserts))

    def test_una_clave_que_ya_existe_no_se_vuelve_a_sembrar_ni_se_pisa(self):
        self._env(existentes=("garantia", "regalia", "evento"))()
        sembradas = {p[0] for _, p in self.inserts}
        self.assertEqual(len(sembradas), 8)
        self.assertNotIn("evento", sembradas)
        self.assertFalse(any("UPDATE" in s for s, _ in self.inserts))

    def test_un_ddl_que_falla_jamas_rompe_el_arranque_ni_deja_de_sembrar(self):
        self._env(falla_tabla=True)()
        self.assertTrue(self.ddl)

    def test_cada_ddl_es_una_sola_clausula(self):
        self._env()()
        for s in self.ddl:
            if s.startswith("ALTER"):
                self.assertEqual(s.count("ADD COLUMN"), 1, s)


class TestElCatalogo(unittest.TestCase):
    def _amb(self, filas=None, falla=False):
        self.consultas = 0

        def fetchall(sql, p=None):
            self.consultas += 1
            if falla:
                raise RuntimeError("no existe la tabla")
            return filas
        return cargar(funciones=("_ot_intenciones_catalogo", "_ot_intenciones_invalidar"),
                      constantes=("_OT_INTEN_TTL_S", "_ot_inten_cache", "_OT_INTEN_COLS"),
                      extra={"_oint": I, "mysql_fetchall": fetchall, "time": time, "print": lambda *a, **k: None})

    def test_si_la_tabla_no_existe_rige_la_semilla(self):
        a = self._amb(falla=True)
        self.assertEqual({f["clave"] for f in a["_ot_intenciones_catalogo"]()}, {f["clave"] for f in CAT})

    def test_lee_el_catalogo_de_la_base_y_lo_recuerda_unos_segundos(self):
        a = self._amb(filas=[{"clave": "marketing", "nombre": "Marketing paga", "descripcion": "", "familia_cero": "regalia",
                              "aplica_a": "cobertura,cerrar_con_perdida", "asume_centro": "marketing", "solo_admin": 1, "activa": 1,
                              "sistema": 0, "orden": 5}])
        c1 = a["_ot_intenciones_catalogo"]()
        a["_ot_intenciones_catalogo"]()
        self.assertEqual(self.consultas, 1)
        self.assertEqual((c1[0]["nombre"], c1[0]["aplica_a"], c1[0]["solo_admin"]), ("Marketing paga", ["cobertura", "cerrar_con_perdida"], True))
        a["_ot_intenciones_invalidar"]()                                  # al editar, la memoria se olvida
        a["_ot_intenciones_catalogo"]()
        self.assertEqual(self.consultas, 2)

    def test_forzar_relee(self):
        a = self._amb(filas=[])
        a["_ot_intenciones_catalogo"]()
        a["_ot_intenciones_catalogo"](forzar=True)
        self.assertEqual(self.consultas, 2)

    def test_una_tabla_vacia_tambien_cae_a_la_semilla(self):
        a = self._amb(filas=[])
        self.assertEqual(len(a["_ot_intenciones_catalogo"]()), 11)


class TestValidarEnElServidor(unittest.TestCase):
    def _f(self, admin=False, cat=None):
        return cargar(funciones=("_ot_intencion_validar",),
                      extra={"_oint": I, "_ot_intenciones_catalogo": lambda forzar=False: (cat if cat is not None else CAT),
                             "_ot_aut_es_admin": lambda: admin, "jsonify": lambda d: d})["_ot_intencion_validar"]

    def test_un_error_es_un_400_con_su_codigo(self):
        resp, fila = self._f()("", "cerrar_sin_documento")
        self.assertEqual(resp[1], 400)
        self.assertEqual((resp[0]["ok"], resp[0]["error_codigo"]), (False, "INTENCION_REQUERIDA"))
        self.assertIsNone(fila)

    def test_la_valida_pasa(self):
        resp, fila = self._f()("marketing", "cerrar_con_perdida")
        self.assertIsNone(resp)
        self.assertEqual(fila["clave"], "marketing")

    def test_el_rol_se_lee_del_servidor_nunca_del_navegador(self):
        cat = _cat(marketing={"solo_admin": True})
        self.assertEqual(self._f(admin=False, cat=cat)("marketing", "cerrar_con_perdida")[0][0]["error_codigo"], "INTENCION_SOLO_ADMIN")
        self.assertIsNone(self._f(admin=True, cat=cat)("marketing", "cerrar_con_perdida")[0])


class TestLaOtQuedaClasificada(unittest.TestCase):
    class Cur:
        def __init__(self, fila_aut=None, falla=False):
            self.sql, self.fila_aut, self.falla = [], fila_aut or {}, falla

        def execute(self, sql, params=None):
            self.sql.append((" ".join(sql.split()), params))
            if self.falla:
                raise RuntimeError("Unknown column 'intencion'")

        def fetchone(self):
            return self.fila_aut

    def _amb(self):
        self.mysql = []
        return cargar(funciones=("_ot_intencion_poner", "_ot_intencion_asentar"),
                      extra={"mysql_execute": lambda sql, p=None: self.mysql.append((" ".join(sql.split()), p)), "print": lambda *a, **k: None})

    def test_la_primera_clasificacion_manda(self):
        a, cur = self._amb(), self.Cur()
        a["_ot_intencion_poner"](cur, 441, " Marketing ")
        self.assertEqual(cur.sql[0], ("UPDATE mant_visitas SET intencion=COALESCE(intencion,%s) WHERE id=%s", ("marketing", 441)))

    def test_un_cero_que_se_declara_ahora_reemplaza(self):
        a, cur = self._amb(), self.Cur()
        a["_ot_intencion_poner"](cur, 441, "evento", pisar=True)
        self.assertEqual(cur.sql[0], ("UPDATE mant_visitas SET intencion=%s WHERE id=%s", ("evento", 441)))

    def test_sin_cursor_usa_la_conexion_normal(self):
        a = self._amb()
        a["_ot_intencion_poner"](None, 441, "evento")
        self.assertEqual(len(self.mysql), 1)

    def test_sin_clave_no_escribe_nada(self):
        a, cur = self._amb(), self.Cur()
        a["_ot_intencion_poner"](cur, 441, "")
        a["_ot_intencion_poner"](cur, 0, "evento")
        self.assertEqual(cur.sql, [])

    def test_si_la_columna_aun_no_existe_jamas_rompe(self):
        a, cur = self._amb(), self.Cur(falla=True)
        a["_ot_intencion_poner"](cur, 441, "evento")        # no lanza
        a["_ot_intencion_asentar"](cur, 441, "evento")      # tampoco

    def test_asentar_usa_la_de_la_autorizacion_con_que_nacio(self):
        a, cur = self._amb(), self.Cur(fila_aut={"intencion": "comercial_asume"})
        a["_ot_intencion_asentar"](cur, 441, None, aut_id=9)
        sqls = [s for s, _ in cur.sql]
        self.assertTrue(sqls[0].startswith("SELECT intencion FROM mant_ot_autorizaciones"))
        self.assertIn(("UPDATE mant_visitas SET intencion=COALESCE(intencion,%s) WHERE id=%s", ("comercial_asume", 441)), cur.sql)
        # y la autorización de $0 que el superadministrador registró al crear la OT también queda clasificada
        self.assertTrue(any("UPDATE mant_ot_autorizaciones SET intencion=COALESCE(intencion,%s)" in s and "cobro_cero_autorizacion_id" in s for s in sqls))

    def test_lo_declarado_manda_sobre_la_autorizacion_y_el_respaldo_es_la_familia(self):
        a, cur = self._amb(), self.Cur(fila_aut={"intencion": "comercial_asume"})
        a["_ot_intencion_asentar"](cur, 441, "evento", aut_id=9)
        self.assertIn(("UPDATE mant_visitas SET intencion=COALESCE(intencion,%s) WHERE id=%s", ("evento", 441)), cur.sql)
        a, cur = self._amb(), self.Cur()
        a["_ot_intencion_asentar"](cur, 442, None, None, respaldo="Garantia")
        self.assertIn(("UPDATE mant_visitas SET intencion=COALESCE(intencion,%s) WHERE id=%s", ("garantia", 442)), cur.sql)
        a, cur = self._amb(), self.Cur()
        a["_ot_intencion_asentar"](cur, 443, None, None)
        self.assertEqual(cur.sql, [])


class TestLaInformacionQueSeMuestra(unittest.TestCase):
    def _amb(self):
        return cargar(funciones=("_ot_intencion_info", "_ot_intencion_campos", "_ot_intencion_frase"),
                      extra={"_oint": I, "_ot_intenciones_catalogo": lambda forzar=False: CAT,
                             "_OT2_CENTROS_COSTO": (("sstt", "Servicio Técnico"), ("comercial", "Comercial"), ("marketing", "Marketing"))})

    def test_quien_lo_asume_es_el_centro_de_la_ot_o_el_que_sugiere_la_clasificacion(self):
        a = self._amb()
        i = a["_ot_intencion_info"]("marketing", "comercial")
        self.assertEqual((i["nombre"], i["asume_centro"], i["asume_txt"]), ("Costo asumido por Marketing", "comercial", "Comercial"))
        self.assertEqual(a["_ot_intencion_info"]("marketing")["asume_txt"], "Marketing")
        self.assertIsNone(a["_ot_intencion_info"]("no_existe"))
        self.assertEqual(a["_ot_intencion_campos"]("no_existe"), {"intencion_txt": "", "asume_txt": "", "asume_centro": None})

    def test_la_frase_del_recorrido(self):
        a = self._amb()
        self.assertEqual(a["_ot_intencion_frase"]("comercial_asume", "comercial"),
                         " Por qué: Costo asumido en el precio de Comercial · lo asume Comercial.")
        self.assertEqual(a["_ot_intencion_frase"](None), "")
        self.assertEqual(a["_ot_intencion_frase"]("garantia", None), " Por qué: Garantía · lo asume Servicio Técnico.")


class TestElInsertDeLaAutorizacion(unittest.TestCase):
    class Cur:
        def __init__(self, sin_columnas=False):
            self.llamadas, self.sin_columnas, self.lastrowid = [], sin_columnas, 55

        def execute(self, sql, params=None):
            self.llamadas.append((" ".join(sql.split()), params))
            if self.sin_columnas and "intencion" in sql:
                raise RuntimeError("(1054, \"Unknown column 'intencion' in 'field list'\")")

    def _f(self):
        return cargar(funciones=("_ot_aut_insertar",), extra={"get_mysql": None, "print": lambda *a, **k: None})["_ot_aut_insertar"]

    def test_guarda_la_intencion_y_el_mensaje_del_sistema(self):
        cur = self.Cur()
        aid = self._f()(cur, {"tipo": "cerrar_con_perdida", "visita_id": 3, "argumento": "x" * 40, "intencion": "marketing",
                              "mensaje_sistema": "La OT cierra con una pérdida"})
        self.assertEqual(aid, 55)
        sql, params = cur.llamadas[0]
        self.assertIn("intencion, mensaje_sistema", sql)
        self.assertEqual(sql.count("%s"), 20)
        self.assertEqual(params[-2:], ("marketing", "La OT cierra con una pérdida"))

    def test_si_las_columnas_aun_no_existen_la_solicitud_se_guarda_igual(self):
        cur = self.Cur(sin_columnas=True)
        aid = self._f()(cur, {"tipo": "cerrar_sin_documento", "visita_id": 3, "argumento": "x" * 40, "intencion": "otro"})
        self.assertEqual(aid, 55)
        self.assertEqual(len(cur.llamadas), 2)
        self.assertNotIn("intencion", cur.llamadas[1][0])
        self.assertEqual(cur.llamadas[1][0].count("%s"), 18)

    def test_otro_error_no_se_traga(self):
        """Un error que no es de las columnas nuevas (un deadlock) sube: no se reintenta con el INSERT viejo a ciegas."""
        class Mala(self.Cur):
            def execute(self, sql, params=None):
                self.llamadas.append((sql, params))
                if "intencion" in sql:
                    raise RuntimeError("deadlock")        # solo el primer intento falla: si se «reintenta», el segundo pasaría y el error se perdería
        mala = Mala()
        with self.assertRaises(RuntimeError):
            self._f()(mala, {"tipo": "cerrar_sin_documento", "argumento": "x"})
        self.assertEqual(len(mala.llamadas), 1)

    def test_el_mensaje_del_sistema_se_acota_al_ancho_de_la_columna(self):
        cur = self.Cur()
        self._f()(cur, {"tipo": "cerrar_con_perdida", "argumento": "x" * 40, "mensaje_sistema": "m" * 900})
        self.assertEqual(len(cur.llamadas[0][1][-1]), 600)


class TestPedirUnaAutorizacion(unittest.TestCase):
    """POST /ot/api/autorizaciones: cada solicitud de una OT lleva su «¿Por qué?» validado por el SERVIDOR."""

    def _amb(self, body, admin=False, cat=None):
        self.guardadas, self.notificadas, self.logs = [], [], []

        def insertar(cur, campos):
            self.guardadas.append(campos)
            return 77

        def fetchone(sql, params=None):
            if "FROM mant_visitas WHERE id=%s" in sql:
                return {"id": params[0], "cliente_id": 5, "estado": "creada", "numero_ot": "OT-2026-00300"}
            return None
        amb = cargar(funciones=("ot_aut_api_crear", "_ot_aut_validar", "_ot_intencion_validar"),
                     constantes=("_OT_AUT_TIPOS", "_OT_AUT_ENTIDADES", "_OT_COBRO_CERO_MOTIVOS", "_OT_AUT_ARGUMENTO_MIN", "_OT_AUT_TIPO_TXT",
                                 "_OT2_CENTROS_COSTO"),
                     extra={"_oint": I, "json": json, "_saldo": SALDO, "jsonify": lambda d: d,
                            "request": type("R", (), {"get_json": staticmethod(lambda silent=True: body)})(),
                            "_ot_intenciones_catalogo": lambda forzar=False: (cat if cat is not None else CAT), "_ot_aut_es_admin": lambda: admin,
                            "mysql_fetchone": fetchone, "_ot_aut_usuario": lambda: (7, "Aarón Urbina"), "_ot_aut_insertar": insertar,
                            "_ot_aut_fila": lambda aid: dict(self.guardadas[-1], id=aid), "_ot_aut_json": lambda a: a,
                            "_mant_log": lambda *a: self.logs.append(a),
                            "_ot_aut_notificar_superadmins": lambda a: (self.notificadas.append(a) or 1),
                            "print": lambda *a, **k: None})
        return amb["ot_aut_api_crear"]

    ARG = "El cliente pidió la visita para una demostración de marca y Marketing la asume."

    def test_sin_el_por_que_no_se_pide_nada(self):
        r = self._amb({"tipo": "cerrar_sin_documento", "visita_id": 300, "argumento": self.ARG})()
        self.assertEqual((r[1], r[0]["error_codigo"]), (400, "INTENCION_REQUERIDA"))
        self.assertEqual(self.guardadas, [])

    def test_con_el_por_que_queda_guardado_con_el_mensaje_del_sistema(self):
        r = self._amb({"tipo": "cerrar_sin_documento", "visita_id": 300, "argumento": self.ARG, "intencion": "marketing"})()
        self.assertEqual(r[1], 201)
        g = self.guardadas[0]
        self.assertEqual((g["intencion"], g["tipo"], g["visita_id"]), ("marketing", "cerrar_sin_documento", 300))
        self.assertIn("OT-2026-00300 no tiene una factura ni boleta", g["mensaje_sistema"])
        self.assertEqual(g["argumento"], self.ARG)                         # el «mensaje del usuario» va aparte
        self.assertEqual(self.notificadas[0]["intencion"], "marketing")
        self.assertIn("por qué: Costo asumido por Marketing", self.logs[0][3])

    def test_una_clasificacion_que_no_aplica_se_rechaza(self):
        r = self._amb({"tipo": "cerrar_sin_documento", "visita_id": 300, "argumento": self.ARG, "intencion": "mal_cobro"})()
        self.assertEqual((r[1], r[0]["error_codigo"]), (400, "INTENCION_NO_APLICA"))

    def test_solo_administrador_se_valida_con_el_rol_del_servidor(self):
        cat = _cat(marketing={"solo_admin": True})
        r = self._amb({"tipo": "cerrar_sin_documento", "visita_id": 300, "argumento": self.ARG, "intencion": "marketing"}, admin=False, cat=cat)()
        self.assertEqual(r[0]["error_codigo"], "INTENCION_SOLO_ADMIN")
        r = self._amb({"tipo": "cerrar_sin_documento", "visita_id": 300, "argumento": self.ARG, "intencion": "marketing"}, admin=True, cat=cat)()
        self.assertEqual(r[1], 201)

    def test_la_perdida_dice_cuanto_en_el_mensaje_del_sistema(self):
        fp = {"perdida": 77899, "cobre": 302101, "me_cobraron": 380000}
        amb = self._amb({"tipo": "cerrar_con_perdida", "visita_id": 300, "argumento": self.ARG, "intencion": "comercial_asume"})
        amb.__globals__.update({"_fac_familia_de": lambda vid: None, "_fac_perdida_de_ot": lambda vid: fp, "_fac_equilibrio_ot": lambda vid, pago=None: {}})
        r = amb()
        self.assertEqual(r[1], 201)
        self.assertIn("pérdida de $77.899", self.guardadas[0]["mensaje_sistema"])
        self.assertEqual(self.guardadas[0]["intencion"], "comercial_asume")

    def test_cobrar_sobre_el_saldo_clasifica_como_descuadre_de_precio(self):
        amb = self._amb({"tipo": "exceder_saldo", "visita_id": 300, "argumento": self.ARG, "intencion": "descuadre_precio",
                         "saldo_pedido": {"servicio": 120000}})
        r = amb()
        self.assertEqual(r[1], 201)
        self.assertIn("servicio $120.000", self.guardadas[0]["mensaje_sistema"])

    def test_un_cero_se_clasifica_como_cobertura_y_la_intencion_fija_la_familia(self):
        # «Costo asumido por Marketing» en un $0: la familia contable es Regalía aunque el navegador mande otra cosa
        r = self._amb({"tipo": "cobro_cero", "visita_id": 300, "argumento": self.ARG, "intencion": "marketing", "motivo": "garantia",
                       "centro_costo": "marketing"})()
        self.assertEqual(r[1], 201)
        self.assertEqual((self.guardadas[0]["motivo"], self.guardadas[0]["intencion"]), ("regalia", "marketing"))
        self.assertIn("como Regalía", self.guardadas[0]["mensaje_sistema"])

    def test_garantia_sigue_siendo_garantia(self):
        r = self._amb({"tipo": "cobro_cero", "visita_id": 300, "argumento": self.ARG, "intencion": "garantia", "centro_costo": "sstt"})()
        self.assertEqual(r[1], 201)
        self.assertEqual((self.guardadas[0]["motivo"], self.guardadas[0]["intencion"]), ("garantia", "garantia"))

    def test_crear_una_ot_en_cero_lleva_la_clasificacion_dentro_de_lo_que_se_creara(self):
        body = {"tipo": "crear_sin_documento", "motivo": "garantia", "intencion": "comercial_asume", "argumento": self.ARG,
                "cliente_id": 5, "payload": {"cliente_id": 5, "tipo_ot": "instalacion", "finanzas": {"centro_costo": "comercial"}}}
        r = self._amb(body)()
        self.assertEqual(r[1], 201)
        pl = json.loads(self.guardadas[0]["payload_json"])
        self.assertEqual((pl["finanzas"]["intencion"], pl["finanzas"]["cobro_cero_motivo"]), ("comercial_asume", "regalia"))

    def test_crear_sin_documento_todavia_se_clasifica_por_su_propia_situacion(self):
        body = {"tipo": "crear_sin_documento", "intencion": "documento_en_tramite", "argumento": self.ARG, "cliente_id": 5,
                "payload": {"cliente_id": 5, "tipo_ot": "instalacion", "finanzas": {}}}
        self.assertEqual(self._amb(body)()[1], 201)
        self.assertNotIn("intencion", json.loads(self.guardadas[0]["payload_json"]).get("finanzas", {}))     # no es un $0
        self.assertEqual(self.guardadas[0]["intencion"], "documento_en_tramite")

    def test_un_ticket_o_una_cotizacion_pueden_pedir_sin_clasificar(self):
        body = {"tipo": "cerrar_sin_documento", "entidad": "ticket", "visita_id": 300, "argumento": self.ARG}
        self.assertEqual(self._amb(body)()[1], 201)
        self.assertIsNone(self.guardadas[0]["intencion"])


class TestElCeroDeclaradoEnUnaOtYaCreada(unittest.TestCase):
    def _amb(self, superadmin, intencion=None, admin=True, cat=None):
        self.reg, self.guardadas = [], []
        amb = cargar(funciones=("_ot_cobro_cero_desde_peticion", "_ot_aut_validar", "_ot_intencion_validar"),
                     constantes=("_OT_AUT_TIPOS", "_OT_COBRO_CERO_MOTIVOS", "_OT_AUT_ARGUMENTO_MIN", "_OT2_CENTROS_COSTO"),
                     extra={"_oint": I, "jsonify": lambda d: d, "_ot_aut_es_superadmin": lambda: superadmin,
                            "_ot_intenciones_catalogo": lambda forzar=False: (cat if cat is not None else CAT), "_ot_aut_es_admin": lambda: admin,
                            "_ot_cobro_cero_registrar": lambda *a, **k: (self.reg.append((a, k)) or 31),
                            "mysql_fetchone": lambda sql, p=None: ({"cliente_id": 5} if "cliente_id" in sql else None),
                            "_ot_aut_usuario": lambda: (7, "Aarón"),
                            "_ot_aut_insertar": lambda cur, c: (self.guardadas.append(c) or 88),
                            "_ot_aut_fila": lambda aid: dict(self.guardadas[-1], id=aid), "_mant_log": lambda *a: None,
                            "_ot_aut_notificar_superadmins": lambda a: 1, "print": lambda *a, **k: None})
        return amb["_ot_cobro_cero_desde_peticion"]

    D = {"cobro_cero_motivo": "garantia", "cobro_cero_argumento": "Equipo con 8 meses de uso, falla de fábrica cubierta por el proveedor.",
         "centro_costo": "sstt"}

    def test_el_superadministrador_declara_con_su_clasificacion(self):
        resp, aid = self._amb(True)(441, dict(self.D, intencion="evento", cobro_cero_motivo="garantia"))
        self.assertIsNone(resp)
        a, k = self.reg[0]
        self.assertEqual((a[1], k["intencion"]), ("regalia", "evento"))         # la clasificación fija la familia

    def test_sin_clasificacion_la_propia_familia_es_la_clasificacion(self):
        self._amb(True)(441, dict(self.D))
        self.assertEqual(self.reg[0][1]["intencion"], "garantia")
        self._amb(True)(441, dict(self.D, cobro_cero_motivo="arriendo_leasing"))      # (cada _amb arranca con su propia lista)
        self.assertEqual(self.reg[0][1]["intencion"], "arriendo_leasing")

    def test_el_resto_pide_autorizacion_con_su_clasificacion_y_mensaje(self):
        resp, aid = self._amb(False)(441, dict(self.D, intencion="comercial_asume"))
        self.assertEqual(resp[1], 202)
        g = self.guardadas[0]
        self.assertEqual((g["intencion"], g["motivo"], g["tipo"]), ("comercial_asume", "regalia", "cobro_cero"))
        self.assertIn("se declara en $0 como Regalía", g["mensaje_sistema"])

    def test_una_clasificacion_invalida_se_rechaza_en_todos_los_caminos(self):
        resp, aid = self._amb(True)(441, dict(self.D, intencion="mal_cobro"))
        self.assertEqual((resp[1], resp[0]["error_codigo"]), (400, "INTENCION_NO_APLICA"))
        self.assertEqual(self.reg, [])


class TestRegistrarUnCeroAutorizado(unittest.TestCase):
    class Cur:
        lastrowid = 0

        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append((" ".join(sql.split()), params))

        def fetchone(self):
            return {"cliente_id": 5}

    def _amb(self):
        self.guardadas, self.puestas = [], []
        return cargar(funciones=("_ot_cobro_cero_registrar",),
                      constantes=("_OT_COBRO_CERO_MOTIVOS", "_OT_COBRO_CERO_MODALIDAD"),
                      extra={"_oint": I, "datetime": __import__("datetime").datetime, "_ot_aut_usuario": lambda: (1, "Daniel Aguilar"),
                             "_ot_aut_insertar": lambda cur, c: (self.guardadas.append(c) or 9),
                             "_ot_intencion_poner": lambda cur, vid, clave, pisar=False: self.puestas.append((vid, clave, pisar)),
                             "_ot_intencion_info": lambda clave, centro=None: ({"nombre": "Costo asumido por Marketing", "asume_txt": "Marketing"}
                                                                               if clave == "marketing" else None),
                             "_ot_fin_clp": lambda n: "$%d" % n, "current_username": lambda: "daniel", "print": lambda *a, **k: None})["_ot_cobro_cero_registrar"]

    def test_la_autorizacion_y_la_ot_quedan_con_la_clasificacion_y_la_bitacora_la_cuenta(self):
        cur = self.Cur()
        aid = self._amb()(441, "regalia", "Marketing se hace cargo del servicio para la demostración de marca.", "marketing", None, cur=cur,
                          intencion="marketing")
        self.assertEqual(aid, 9)
        self.assertEqual((self.guardadas[0]["intencion"], self.guardadas[0]["tipo"]), ("marketing", "cobro_cero"))
        self.assertEqual(self.puestas, [(441, "marketing", True)])
        log = [p for s, p in cur.sql if s.startswith("INSERT INTO mant_logs")][0]
        self.assertIn("por qué: Costo asumido por Marketing (lo asume Marketing)", log[2])

    def test_la_de_la_autorizacion_aprobada_se_usa_si_no_viene_otra_y_si_no_hay_ninguna_la_familia(self):
        cur = self.Cur()
        self._amb()(441, "regalia", "x" * 40, None, None, cur=cur, autorizacion={"id": 4, "resuelto_por_nombre": "Daniel", "intencion": "marketing"})
        self.assertEqual(self.puestas[-1][1], "marketing")
        cur = self.Cur()
        self._amb()(441, "garantia", "x" * 40, None, None, cur=cur)
        self.assertEqual(self.puestas[-1][1], "garantia")


# ───────────────────────── cableado en app.py ─────────────────────────
class TestCableado(unittest.TestCase):
    def test_las_rutas_nuevas_excluyen_al_tecnico_y_la_gestion_es_del_superadmin(self):
        for n in ("ot_intenciones_api_listar", "ot_intenciones_api_gestion", "ot_intenciones_api_crear", "ot_intenciones_api_editar",
                  "ot_intenciones_api_estado", "ot_intenciones_pagina"):
            self.assertIn("_no_tecnico", ",".join(decoradores(n)), n)
            self.assertIn("_mant_required", ",".join(decoradores(n)), n)
        for n in ("ot_intenciones_api_gestion", "ot_intenciones_api_crear", "ot_intenciones_api_editar", "ot_intenciones_api_estado",
                  "ot_intenciones_pagina"):
            self.assertIn("_ot_solo_superadmin", ",".join(decoradores(n)), n)

    def test_el_decorador_de_superadmin_existe_antes_de_la_primera_ruta_que_lo_usa(self):
        t = texto()
        self.assertLess(t.index("def _ot_solo_superadmin(view):"), t.index("def ot_intenciones_api_gestion("))

    def test_el_arranque_las_crea_siempre_despues_de_las_autorizaciones(self):
        t = texto()
        self.assertLess(t.index("_ensure_ot_autorizaciones()\nexcept"), t.index("_ensure_ot_intenciones()\nexcept"))
        self.assertIn("with app.app_context():\n        _ensure_ot_intenciones()", t)

    def test_toda_solicitud_se_valida_antes_de_guardarse_y_con_la_familia_derivada(self):
        s = fuente_funcion("ot_aut_api_crear")
        self.assertLess(s.index("_ot_intencion_validar("), s.index("_ot_aut_validar(tipo, _motivo_in"))
        self.assertLess(s.index("_ot_intencion_validar("), s.index("_ot_aut_insertar("))
        self.assertIn('"intencion": intencion, "mensaje_sistema": _ms_txt', s)
        self.assertIn('payload["finanzas"]["intencion"] = intencion', s)

    def test_el_json_de_una_autorizacion_trae_el_por_que_el_quien_y_el_mensaje(self):
        s = fuente_funcion("_ot_aut_json")
        for pista in ('"intencion": r.get("intencion")', '"mensaje_sistema": r.get("mensaje_sistema")', "**_ot_intencion_campos("):
            self.assertIn(pista, s)

    def test_aprobar_deja_la_clasificacion_en_la_constancia_y_en_la_ot(self):
        s = fuente_funcion("ot_aut_api_aprobar")
        self.assertIn("Clasificación:", s)
        self.assertIn("_ot_intencion_poner(cur, vid, a.get(\"intencion\"))", s)
        self.assertIn("_ot_intencion_poner(None, vid_nuevo, a.get(\"intencion\"))", s)

    def test_el_asistente_valida_y_asienta_la_clasificacion_de_la_ot(self):
        self.assertIn("_ot_intencion_asentar(cur, vid, _fin_campos.get(\"intencion\")", fuente_funcion("_ot2_crear_core"))
        s = fuente_funcion("_ot_validar_normalizar_finanzas")
        self.assertIn('"cobertura", True, requerida=False', s)
        self.assertIn('"intencion": _fin_intencion,', s)
        self.assertIn("INTENCION_MOTIVO", s)

    def test_la_desinstalacion_hereda_el_por_que_del_cero_de_su_instalacion(self):
        """Una desinstalación cubierta no queda «Sin clasificar» junto a una instalación clasificada (el mes se decide con las dos)."""
        self.assertIn('"intencion": None,', fuente_funcion("_ot_fin_campos_cubierta"))          # misma forma que el validador normal
        s = fuente_funcion("_ot2_crear_core")
        i = s.index("SET h.intencion=COALESCE(h.intencion, m.intencion)")
        self.assertIn("m.cobro_cero_motivo IS NOT NULL AND m.intencion IS NOT NULL", s[i:i + 200])   # solo si la instalación ES un $0
        self.assertLess(s.rindex("if es_hija:", 0, i), i)                                               # y solo para la hija
        self.assertIn("except Exception as _e_ih:", s[i:i + 600])                                       # nunca rompe la creación

    def test_el_recorrido_cuenta_el_por_que_y_quien_lo_asume_en_la_misma_frase(self):
        s = fuente_funcion("ot_api_recorrido")
        self.assertIn("_ot_intencion_frase(", s)
        self.assertIn("+ _txt_inten}", s)
        self.assertIn("+ _txt_inten_cerrar}", s)

    def test_el_excel_trae_el_por_que_y_la_hoja_por_mes(self):
        s = fuente_funcion("ot2_reporte_xlsx")
        for pista in ('"Por qué (clasificación)"', '"Cuándo autorizó"', 'wb.create_sheet("Por mes e intención")', "_oint.resumen_por_mes("):
            self.assertIn(pista, s)
        self.assertIn("Centro de costo (quién lo asume)", s)             # en la hoja nueva
        self.assertIn('_h6d = ["Cobertura", "Centro de costo", "N° OT",', s)   # el detalle de siempre conserva su encabezado (una tabla dinámica puede depender de él)

    def test_la_campana_y_el_correo_interno_llevan_la_clasificacion(self):
        s = fuente_funcion("_ot_aut_notificar_superadmins")
        self.assertIn("Por qué (clasificación)", s)
        self.assertIn("Lo que detectó el sistema", s)
        # nada nuevo le llega al cliente (REGLA #22): el aviso sigue siendo a los superadministradores
        self.assertIn("_ot_aut_receptores()", s)

    def test_la_unica_puerta_de_escritura_de_autorizaciones_sigue_siendo_una(self):
        fuera = texto().replace(fuente_funcion("_ot_aut_insertar"), "")
        self.assertNotIn("INSERT INTO mant_ot_autorizaciones", fuera)       # solo _ot_aut_insertar escribe una solicitud nueva


class TestLosTecnicosNoVenNada(unittest.TestCase):
    def test_ninguna_ruta_de_intenciones_es_para_un_tecnico(self):
        t = texto()
        for m in re.finditer(r'@app\.route\("(/ot/api/intenciones[^"]*|/ot/intenciones)"[^)]*\)\n((?:@\w+\n)+)def (\w+)', t.replace("\r\n", "\n")):
            self.assertIn("@_no_tecnico", m.group(2), m.group(3))

    def test_la_sugerida_y_el_catalogo_solo_salen_para_gestion(self):
        s = fuente_funcion("ot_intenciones_api_listar")
        self.assertIn("_ot_aut_es_admin()", s)
        self.assertIn("es_admin or not f[\"solo_admin\"]", s)


# ───────────────────────── pantallas (JS en Node y plantillas) ─────────────────────────
def _js_funcion(txt, nombre):
    i = txt.index("function %s(" % nombre)
    prof, j = 0, txt.index("{", i)
    while True:
        if txt[j] == "{":
            prof += 1
        elif txt[j] == "}":
            prof -= 1
            if prof == 0:
                return txt[i:j + 1]
        j += 1


def _node(programa):
    f = os.path.join(tempfile.mkdtemp(), "prog.js")
    with open(f, "w", encoding="utf-8") as fh:
        fh.write(programa)
    r = subprocess.run(["node", f], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElAsistente(unittest.TestCase):
    W = _leer("templates/ot2/_modal_crear.html")

    def _prog(self, estado, cuerpo, funciones):
        piezas = ["var S = %s;" % json.dumps(estado),
                  "function esc(s){ return String(s==null?'':s); }",
                  "var _O2M_INT_RESPALDO = %s;" % json.dumps([
                      {"clave": "garantia", "nombre": "Garantía", "familia_cero": "garantia", "aplica_a": ["cobertura"]},
                      {"clave": "regalia", "nombre": "Regalía", "familia_cero": "regalia", "aplica_a": ["cobertura"]},
                      {"clave": "otro", "nombre": "Otro motivo", "familia_cero": None, "aplica_a": ["cobertura", "crear_sin_documento", "exceder_saldo"]}]),
                  "var _O2M_INT = {items:null, cargando:false, sug:{}};"]
        piezas += [_js_funcion(self.W, f) for f in funciones]
        piezas.append(cuerpo)
        return "\n".join(piezas)

    EST = {"fin_int": {"cobertura": "garantia", "crear_sin_documento": "", "exceder_saldo": "", "documento_observado": ""},
           "_intElegido": {}, "_intSug": {}, "fin_cero_motivo": "garantia", "fin_centro": None}

    def test_elegir_una_clasificacion_de_cero_fija_la_familia_contable(self):
        est = json.loads(json.dumps(self.EST))
        prog = self._prog(est, "\n".join([
            "_O2M_INT.items = [{clave:'marketing', nombre:'Costo asumido por Marketing', familia_cero:'regalia', aplica_a:['cobertura']}];",
            "function _o2mFinEstadosDom(){} function _pintarFinGuiaDom(){} function refrescarAnclas(){}",
            "var document = {getElementById: function(){ return null; }};",
            "setFinCeroMotivo('marketing');",
            "var a = {motivo: S.fin_cero_motivo, int: S.fin_int.cobertura, elegido: !!S._intElegido.cobertura};",
            "setFinCeroMotivo('garantia'); a.garantia = S.fin_cero_motivo;",
            "setFinCeroMotivo('arriendo_leasing'); a.base = S.fin_cero_motivo;",
            "console.log(JSON.stringify(a));"]), ["_o2mIntFamilia", "setFinIntencion", "setFinCeroMotivo", "_o2mIntOrigenTxt"])
        r = _node(prog)
        self.assertEqual(r, {"motivo": "regalia", "int": "marketing", "elegido": True, "garantia": "garantia", "base": "arriendo_leasing"})

    def test_sin_catalogo_quedan_las_tres_familias_y_otro_motivo(self):
        prog = self._prog(json.loads(json.dumps(self.EST)), "\n".join([
            "console.log(JSON.stringify({co: _o2mIntOpciones('cobertura').map(function(i){return i.clave;}), sd: _o2mIntOpciones('crear_sin_documento').map(function(i){return i.clave;})}));"]),
            ["_o2mIntOpciones"])
        r = _node(prog)
        self.assertEqual(r, {"co": ["garantia", "regalia", "otro"], "sd": ["otro"]})

    def test_el_selector_se_pinta_con_lo_elegido_y_dice_el_origen_de_lo_sugerido(self):
        est = json.loads(json.dumps(self.EST))
        est["fin_int"]["cobertura"] = "regalia"
        est["_intSug"] = {"cobertura": {"clave": "regalia", "origen": "Sugerida según su centro de costo (Comercial)"}}
        prog = self._prog(est, "\n".join([
            "function _o2mIntCargar(){} function _o2mIntSugerir(){}",
            "var h = _o2mIntSelectHtml('cobertura', 'Motivo', true);",
            "console.log(JSON.stringify({h: h}));"]),
            ["_o2mIntOpciones", "_o2mIntOpcionesHtml", "_o2mIntOrigenTxt", "_o2mIntSelectHtml"])
        h = _node(prog)["h"]
        self.assertIn('<option value="regalia" selected>Regalía</option>', h)
        self.assertIn("Sugerida según su centro de costo (Comercial): puedes cambiarla.", h)
        self.assertIn("OT2C.setFinIntencion('cobertura', this.value)", h)
        self.assertIn("min-height:44px", h)                      # botón táctil (REGLA #3)

    def test_la_etiqueta_nunca_miente_sin_documento_todavia_pide_el_por_que_en_las_dos_partes(self):
        # completo('finanzas') y _o2mFinSubEstado('cobro') piden EXACTAMENTE lo mismo (REGLA #45)
        w = self.W
        self.assertIn("if (S.fin_aut && !S.fin_garantia && !(S.fin_int && S.fin_int.crear_sin_documento)) return false;", w)
        self.assertIn("if (!(S.fin_int && S.fin_int.crear_sin_documento)) return {cls:'falta', txt:'Falta elegir el motivo'};", w)
        self.assertIn("(S.fin_obs_argumento || '').trim().length < 30 || !(S.fin_int && S.fin_int.documento_observado))", w)

    def test_cada_camino_manda_su_clasificacion_al_servidor(self):
        w = self.W
        self.assertIn("intencion: opts.intencion || (!opts.tipo ? (S.fin_garantia ? S.fin_int.cobertura : S.fin_int.crear_sin_documento) : null) || null", w)
        self.assertIn("{tipo: 'exceder_saldo', argumento: _argSa, intencion: _sa.intencion || S.fin_int.exceder_saldo}", w)
        self.assertIn("intencion: S.fin_int.documento_observado});", w)
        self.assertIn("body.finanzas.intencion = S.fin_int.cobertura", w)
        self.assertIn("setFinIntencion:setFinIntencion,", w)

    def test_el_garantia_de_siempre_sigue_siendo_el_valor_inicial(self):
        self.assertIn("fin_int:{cobertura:'garantia', crear_sin_documento:'', exceder_saldo:'', documento_observado:''}", self.W)
        self.assertIn("var _O2M_CERO_MOTIVOS = [['garantia','Garantía'],['regalia','Regalía'],['arriendo_leasing','Arriendo o leasing']];", self.W)   # no se quitó nada

    def test_no_se_usa_ningun_dialogo_nativo(self):
        i = self.W.index("var _O2M_INT_RESPALDO")
        j = self.W.index("function setFinCobro(")
        bloque = self.W[i:j]
        for prohibido in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(prohibido, bloque.replace("ilusAlert(", "").replace("ilusConfirm(", "").replace("ilusPrompt(", ""))


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElMotorDeLaFicha(unittest.TestCase):
    M = _leer("static/ot_fin_motor.js")

    def test_cada_dialogo_de_autorizacion_pide_su_por_que_y_lo_manda(self):
        m = self.M
        self.assertIn("function cargarIntenciones(uso, centro)", m)
        self.assertIn("function campoPorQue(cat, etiqueta)", m)
        for uso in ("exceder_saldo", "cerrar_con_perdida"):
            self.assertIn("cargarIntenciones('%s'" % uso, m)
        self.assertIn("cargarIntenciones(cero ? 'cobertura' : 'cerrar_sin_documento'", m)
        self.assertIn("intencion: v.intencion", m)
        for cuerpo in ("tipo: 'exceder_saldo', visita_id: inst.vid, argumento: v.argumento, intencion: v.intencion",
                       "tipo: 'cerrar_con_perdida', visita_id: inst.vid, argumento: v.argumento, intencion: v.intencion",
                       "argumento: v.argumento, centro_costo: v.centro_costo, intencion: v.intencion"):       # cerrar sin documento y $0
            self.assertIn(cuerpo, m)
        self.assertIn("body.motivo = familiaDe(cat, v.intencion) || v.intencion;", m)

    def test_el_campo_es_obligatorio_y_trae_la_sugerida_con_su_origen(self):
        prog = "\n".join([
            "function esc(s){ return String(s==null?'':s); }",
            _js_funcion(self.M, "campoPorQue"), _js_funcion(self.M, "familiaDe"),
            "var cat = {items: [{clave:'marketing', nombre:'Costo asumido por Marketing', familia_cero:'regalia'}, {clave:'garantia', nombre:'Garantía', familia_cero:'garantia'}],"
            " sugerida: {clave:'marketing', origen:'Sugerida según su centro de costo (Marketing)'}};",
            "var c = campoPorQue(cat, '¿Por qué?');",
            "console.log(JSON.stringify({req: c.req, valor: c.valor, ops: c.opciones.map(function(o){return o.v;}), ayuda: c.ayuda, fam: familiaDe(cat, 'marketing'), fam2: familiaDe(cat, 'xx')}));"])
        r = _node(prog)
        self.assertEqual((r["req"], r["valor"], r["ops"]), (True, "marketing", ["", "marketing", "garantia"]))
        self.assertIn("Sugerida según su centro de costo (Marketing): puedes cambiarla.", r["ayuda"])
        self.assertEqual((r["fam"], r["fam2"]), ("regalia", None))

    def test_si_el_catalogo_no_responde_queda_otro_motivo_para_no_dejar_la_solicitud_sin_salida(self):
        self.assertIn("var INTEN_RESPALDO = { items: [{ clave: 'otro', nombre: 'Otro motivo', familia_cero: null }], sugerida: null };", self.M)

    def test_el_lote_de_facturacion_de_proveedor_tambien_pide_el_por_que_de_la_perdida(self):
        t = _leer("templates/mantenciones/factura_proveedor_detalle.html")
        self.assertIn("async function fpdElegirPorQue()", t)
        self.assertIn("intencion: intencion})});", t)
        self.assertLess(t.index("const intencion = await fpdElegirPorQue();"), t.index("const arg = await ilusPrompt({title: 'Pedir autorización a Daniel · '"))


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestLaPantallaDeGestion(unittest.TestCase):
    J = _leer("static/ot_intenciones.js")

    def _correr(self, items, extra_js=""):
        prog = """
var html = '';
var cajas = [];
var raiz = {innerHTML: '', _l: {}, addEventListener: function(t, f){ this._l[t] = f; }};
var document = {
  getElementById: function(id){ return id === 'ppInten' ? raiz : (id === 'oiNueva' ? {addEventListener: function(){}} : null); },
  createElement: function(){ return {}; }, addEventListener: function(){}, removeEventListener: function(){}, body: {appendChild: function(){}}
};
var window = {ilusToast: function(){}, ilusConfirm: function(){ return Promise.resolve(true); }};
var fetch = function(url){ return Promise.resolve({ok: true, json: function(){ return Promise.resolve(%s); }}); };
%s
setTimeout(function(){ console.log(JSON.stringify({html: raiz.innerHTML})); }, 20);
""" % (json.dumps({"ok": True, "items": items, "usos": [{"v": "cobertura", "n": "Un $0 (la OT no se cobra)"}, {"v": "cerrar_con_perdida", "n": "Cerrar con pérdida"}],
                   "familias": [{"v": "regalia", "n": "Regalía"}], "centros": [{"v": "marketing", "n": "Marketing"}]}), extra_js)
        f = os.path.join(tempfile.mkdtemp(), "p.js")
        with open(f, "w", encoding="utf-8") as fh:
            fh.write(prog + "\n" + self.J)
        r = subprocess.run(["node", f], capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return json.loads(r.stdout)["html"]

    def test_las_tarjetas_dicen_todo_sin_cortar_nada(self):
        items = [{"clave": "marketing", "nombre": "Costo asumido por Marketing con un nombre bastante largo para comprobar que no se corta", "descripcion": "Marketing se hace cargo",
                  "familia_cero": "regalia", "aplica_a": ["cobertura", "cerrar_con_perdida"], "asume_centro": "marketing", "asume_txt": "Marketing", "solo_admin": True,
                  "activa": True, "sistema": False, "orden": 50, "creada_por": "Daniel", "creada_at": "25/10/2026 10:00", "actualizada_por": "", "actualizada_at": ""},
                 {"clave": "evento", "nombre": "Evento", "descripcion": "", "familia_cero": "regalia", "aplica_a": ["cobertura"], "asume_centro": "",
                  "solo_admin": False, "activa": False, "sistema": False, "orden": 40, "creada_por": "sistema", "creada_at": "", "actualizada_por": "", "actualizada_at": ""},
                 {"clave": "garantia", "nombre": "Garantía", "descripcion": "", "familia_cero": "regalia", "aplica_a": ["cobertura"], "asume_centro": "marketing",
                  "solo_admin": False, "activa": True, "sistema": True, "orden": 10, "creada_por": "sistema", "creada_at": "", "actualizada_por": "", "actualizada_at": ""}]
        h = self._correr(items)
        self.assertIn("Costo asumido por Marketing con un nombre bastante largo para comprobar que no se corta", h)
        for pista in ("Solo administrador", "Desactivada", "Del sistema", "Un $0 · Regalía", "Un $0 (la OT no se cobra)", "Cerrar con pérdida", "Creada por Daniel el 25/10/2026 10:00",
                      "Quién lo asume", "Activas <b>2</b>", "Desactivadas <b>1</b>"):
            self.assertIn(pista, h)
        # una del sistema no se desactiva; una común sí (y una desactivada se puede volver a activar)
        self.assertEqual(h.count('data-estado="'), 2)
        self.assertIn('data-estado="evento" data-activa="1"', h)
        self.assertIn('data-estado="marketing" data-activa="0"', h)
        self.assertNotIn('data-estado="garantia"', h)

    def test_no_usa_dialogos_nativos(self):
        sin = re.sub(r"\bilus(Alert|Confirm|Prompt)\(", "", self.J)
        for prohibido in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(prohibido, sin)

    def test_marcar_o_desmarcar_todo_y_botones_de_44px(self):
        self.assertIn("Marcar / desmarcar todo", self.J)
        css = _leer("static/ot_intenciones.css")
        self.assertIn("min-height:44px", css)
        self.assertIn("font-size:16px", css)

    def test_la_plantilla_es_jinja_valido_y_se_apoya_en_el_formato_de_retiros(self):
        from jinja2 import Environment
        t = _leer("templates/ot2/intenciones.html")
        Environment().parse(t)
        self.assertIn("ot_puerta_paginas.css", t)
        self.assertIn("Nueva clasificación", t)


class TestLaBandejaDeAutorizaciones(unittest.TestCase):
    J = _leer("static/ot_autorizaciones.js")

    def test_cada_tarjeta_dice_por_que_quien_lo_asume_y_lo_que_detecto_el_sistema(self):
        for pista in ("Por qué (clasificación)", "Quién lo asume", "Lo que detectó el sistema", "a.intencion_txt", "a.mensaje_sistema", "a.asume_txt"):
            self.assertIn(pista, self.J)
        self.assertIn("Argumento (completo) · de '", self.J)           # el «mensaje del usuario» va aparte y con el nombre de quien lo escribió

    def test_el_boton_de_gestion_solo_lo_ve_el_superadministrador(self):
        t = _leer("templates/ot2/autorizaciones.html")
        i = t.index("ot_intenciones_pagina")
        self.assertIn("{% if es_superadmin %}", t[i - 120:i])


if __name__ == "__main__":
    unittest.main()

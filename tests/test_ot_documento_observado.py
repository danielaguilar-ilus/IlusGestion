# -*- coding: utf-8 -*-
"""🗓️ 2026-10-12 DOCUMENTO OBSERVADO + CLASIFICACIÓN DE LA OT (Daniel).

Daniel: «si tiene más de tres meses de antigüedad… deberá pedir autorización y declarar un argumento persistente para
que la OT quede guardada con ese antecedente y esa clasificación», «si tiene productos, deberían tener saldo» y «debemos
guardar las clasificaciones, cómo se están trabajando las OT». Respuestas: productos sin saldo → autorización con
argumento; más de 3 meses → Daniel a distancia (o el superadmin que crea, declarando); clasificación → ficha, tabla de
OT y Excel, solo superadmin.

Se prueba: la lógica pura (ot_saldo_servicio.py), el candado del servidor con datos de mentira (nunca contra producción,
REGLA #22), que solo el asistente «Nueva OT» lo revise, que la autorización cubra EXACTAMENTE lo que Daniel vio, que la
clasificación quede guardada y que nadie más que el superadministrador la vea (REGLA #26). Y la guía «Camino al margen»
ejecutando las funciones REALES del asistente en Node.
"""
import datetime as _dt
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import ot_saldo_servicio as S
from tests.test_ot_saldo_servicio import _fuente, _leer
from tests.test_fac_libro_saldo import _exec

HOY = _dt.date(2026, 10, 12)
H = _leer("templates/ot2/_modal_crear.html")
APP = _leer("app.py")


def _l(sku, cant, saldo):
    return {"sku": sku, "cantidad": cant, "saldo": saldo, "es_zz": sku.startswith("ZZ"), "es_descuento": sku == "DE"}


# ───────────────────────── lógica pura ─────────────────────────
class TestFechas(unittest.TestCase):
    def test_formatos_del_erp(self):
        self.assertEqual(S.fecha_doc("10/07/2026"), _dt.date(2026, 7, 10))
        self.assertEqual(S.fecha_doc("2026-07-10"), _dt.date(2026, 7, 10))
        self.assertEqual(S.fecha_doc("2026-07-10T00:00:00"), _dt.date(2026, 7, 10))
        self.assertEqual(S.fecha_doc(_dt.datetime(2026, 7, 10, 15, 0)), _dt.date(2026, 7, 10))
        self.assertIsNone(S.fecha_doc("ayer"))
        self.assertIsNone(S.fecha_doc(""))

    def test_restar_meses_sin_inventar_dias(self):
        self.assertEqual(S.restar_meses(_dt.date(2026, 5, 31), 3), _dt.date(2026, 2, 28))
        self.assertEqual(S.restar_meses(_dt.date(2026, 1, 15), 3), _dt.date(2025, 10, 15))


class TestObservarDocumento(unittest.TestCase):
    def test_justo_tres_meses_no_es_antiguo(self):
        self.assertEqual(S.observar_documento("FCV 1", "12/07/2026", HOY, [_l("MAQ", 1, 1)]), [])

    def test_un_dia_mas_de_tres_meses_pide_autorizacion(self):
        o = S.observar_documento("FCV 1", "11/07/2026", HOY, [_l("MAQ", 1, 1)])
        self.assertEqual([x["codigo"] for x in o], ["antiguo"])
        self.assertIn("FCV 1 es del 11/07/2026 (hace 93 días)", o[0]["texto"])

    def test_productos_todos_sin_saldo(self):
        o = S.observar_documento("FCV 2", "01/10/2026", HOY, [_l("MAQ", 2, 0), _l("BANCA", 1, 0)])
        self.assertEqual([x["codigo"] for x in o], ["productos_sin_saldo"])
        self.assertIn("2 productos", o[0]["texto"])

    def test_un_producto_con_saldo_basta(self):
        self.assertEqual(S.observar_documento("FCV 2", "01/10/2026", HOY, [_l("MAQ", 2, 0), _l("BANCA", 1, 1)]), [])

    def test_servicio_descuento_y_cantidad_cero_no_son_productos(self):
        lineas = [_l("ZZINSTALACION", 1, 0), _l("DE", 5000, 0), _l("MAQ", 0, 0)]
        self.assertEqual(S.productos_con_saldo(lineas)["total"], 0)
        self.assertEqual(S.observar_documento("FCV 3", "01/10/2026", HOY, lineas), [])

    def test_sin_fecha_legible_no_se_inventa(self):
        self.assertEqual(S.observar_documento("FCV 4", None, HOY, [_l("MAQ", 1, 1)]), [])


class TestClasificar(unittest.TestCase):
    def test_lo_mas_delicado_manda(self):
        c = S.clasificar_ot(tidos=["FCV"], observaciones=[{"codigo": "antiguo"}], ajuste_manual=True)
        self.assertEqual(c["codigo"], "documento_antiguo")
        self.assertEqual(c["nivel"], "rojo")
        self.assertEqual(c["otros"], ["Cobro ajustado a mano · con argumento", "Factura con saldo"])

    def test_factura_limpia(self):
        self.assertEqual(S.clasificar_ot(tidos=["FCV"])["texto"], "Factura con saldo")

    def test_nota_de_venta_sola_falta_factura(self):
        self.assertEqual(S.clasificar_ot(tidos=["NVV"])["codigo"], "nota_venta")
        self.assertEqual(S.clasificar_ot(tidos=["NVV", "FCV"])["codigo"], "factura")

    def test_guia_de_despacho_no_es_nota_de_venta(self):   # caso real: OT 449 con GDV 31886
        self.assertEqual(S.clasificar_ot(tidos=["GDV"])["texto"], "Guía u otro documento · falta factura")
        self.assertEqual(S.clasificar_ot(tidos=["GDV", "NVI"])["codigo"], "nota_venta")

    def test_cero_interna_y_sin_documento(self):
        self.assertEqual(S.clasificar_ot(cero_motivo="arriendo_leasing")["texto"], "$0 · arriendo o leasing")
        self.assertEqual(S.clasificar_ot(es_interna=True)["codigo"], "interna")
        self.assertEqual(S.clasificar_ot(sin_documento=True)["codigo"], "sin_documento")
        self.assertIsNone(S.clasificar_ot()["codigo"])


# ───────────────────────── servidor (datos de mentira) ─────────────────────────
class TestObservacionesServidor(unittest.TestCase):
    def _amb(self, docs_erp, familia=None):
        def cached(t, n):
            return docs_erp.get((t, n), (None, []))
        return {"_mant_erp_doc_cached": cached, "_saldo": S, "_ot_doc_real_a_usuario": lambda t, n: (t, n.lstrip("0")),
                "_now_chile": lambda: _dt.datetime(2026, 10, 12, 10, 0),
                "_ot_saldo_familia": lambda t, n: familia.get((t, n), {S.clave_doc(t, n)}) if familia else {S.clave_doc(t, n)},
                "print": lambda *a, **k: None}

    def test_documento_viejo_se_observa(self):
        amb = self._amb({("FCV", "11439"): ({"fecha": "01/06/2026"}, [_l("MAQ", 1, 1)])})
        _exec("_ot_doc_observar", amb)
        f = _exec("_ot_doc_observaciones", amb)
        r = f([("FCV", "11439")])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["clave"], ["FCV", "11439"])
        self.assertEqual(r[0]["observaciones"][0]["codigo"], "antiguo")

    def test_random_caido_no_observa(self):
        amb = self._amb({})
        _exec("_ot_doc_observar", amb)
        self.assertEqual(_exec("_ot_doc_observaciones", amb)([("FCV", "1")]), [])

    def test_nota_de_venta_con_su_factura_no_se_observa(self):
        viejo = ({"fecha": "01/01/2026"}, [_l("MAQ", 1, 0)])
        amb = self._amb({("NVV", "VD00010667"): viejo, ("FCV", "11439"): ({"fecha": "01/10/2026"}, [_l("MAQ", 1, 1)])},
                        familia={("NVV", "VD00010667"): {("VD", "10667"), ("FCV", "11439")}})
        _exec("_ot_doc_observar", amb)
        self.assertEqual(_exec("_ot_doc_observaciones", amb)([("NVV", "VD00010667"), ("FCV", "11439")]), [])


class TestAutorizacionCubreLoQueDanielVio(unittest.TestCase):
    OBS = [{"tido": "FCV", "nudo": "11439", "observaciones": [{"codigo": "antiguo"}]}]

    def _amb(self, fila, usuario=7, sa=False):
        return {"mysql_fetchone": lambda sql, p=None: (fila if "mant_ot_autorizaciones" in sql else {"rut": "76.111.111-1"}),
                "_ot_aut_usuario": lambda: (usuario, "Ana"), "_ot_aut_es_superadmin": lambda: sa,
                "_saldo": S, "json": json, "re": __import__("re"), "print": lambda *a, **k: None}

    def _fila(self, obs, **kw):
        f = {"id": 9, "tipo": "documento_observado", "payload_json": json.dumps({"_observaciones_doc": obs}),
             "visita_id": None, "visita_creada_id": None, "solicitado_por_user_id": 7, "cliente_id": 3}
        f.update(kw)
        return f

    def test_cubre_lo_que_vio(self):
        f = _exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS)))
        self.assertEqual(f(9, "76111111-1", self.OBS)["id"], 9)

    def test_no_cubre_una_observacion_nueva(self):
        f = _exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS)))
        nuevo = [{"tido": "FCV", "nudo": "11439", "observaciones": [{"codigo": "antiguo"}, {"codigo": "productos_sin_saldo"}]}]
        self.assertIsNone(f(9, "76111111-1", nuevo))

    def test_no_sirve_ya_usada_ni_de_otro_usuario_ni_de_otro_cliente(self):
        self.assertIsNone(_exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS, visita_creada_id=55)))(9, None, self.OBS))
        self.assertIsNone(_exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS), usuario=8))(9, None, self.OBS))
        self.assertIsNone(_exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS)))(9, "99.999.999-9", self.OBS))

    def test_el_superadmin_que_aprueba_si_la_usa(self):
        f = _exec("_ot_doc_obs_autorizada", self._amb(self._fila(self.OBS), usuario=1, sa=True))
        self.assertEqual(f(9, None, self.OBS)["id"], 9)


class TestCableadoServidor(unittest.TestCase):
    def test_solo_el_asistente_revisa_el_documento(self):
        v = _fuente("_ot_validar_normalizar_finanzas")
        self.assertIn("revisar_documento=False", v)
        self.assertIn('"DOC_OBSERVADO"', v)
        self.assertIn("if revisar_documento and _OT_DOC_OBS_ACTIVO and _docs_t:", v)
        self.assertIn("revisar_documento=True", _fuente("_ot2_crear_core"))
        self.assertEqual(len(re.findall(r"^\s+revisar_documento=True\)", APP, re.M)), 1)   # una sola llamada: ni histórica, ni retroactiva, ni plan anual

    def test_el_tipo_nuevo_existe_en_la_tabla(self):
        self.assertEqual(APP.count("'cerrar_con_perdida','documento_observado') NOT NULL"), 2)   # CREATE + ALTER
        self.assertIn('"documento_observado")', APP)

    def test_la_solicitud_calcula_las_observaciones_en_el_servidor(self):
        f = _fuente("ot_aut_api_crear")
        self.assertIn('payload.pop("_observaciones_doc", None)', f)   # nunca lo que mande el navegador
        self.assertIn('payload["_observaciones_doc"] = _obs_p', f)
        self.assertIn('if tipo in ("crear_sin_documento", "documento_observado") or', _fuente("ot_aut_api_aprobar"))

    def test_la_clasificacion_se_guarda_al_crear_y_no_traba(self):
        f = _fuente("_ot2_crear_core")
        self.assertIn("_ot_fin_clasif_guardar(cur, vid, _fin_campos, es_interna, autorizacion)", f)
        g = _fuente("_ot_fin_clasif_guardar")
        self.assertIn("UPDATE mant_visitas SET fin_clasif=%s, fin_clasif_json=%s WHERE id=%s", g)
        self.assertIn('"documento_observado_autorizado"', g)

    def test_una_clausula_por_sentencia(self):
        f = _fuente("_ensure_ot_fin_clasif_cols")
        self.assertIn("ADD COLUMN fin_clasif VARCHAR(30) NULL", f)
        self.assertIn("ADD COLUMN fin_clasif_json MEDIUMTEXT NULL", f)
        self.assertNotIn(", ADD COLUMN", f)


class TestSoloSuperadmin(unittest.TestCase):
    """REGLA #26: la clasificación es dato financiero. Tabla, ficha y Excel: solo superadmin, nunca un técnico."""

    def test_tabla(self):
        f = _fuente("_ot2_panel_extras")
        self.assertIn("if not _es_rol_tecnico() and _ot_aut_es_superadmin():", f)

    def test_ficha(self):
        f = _fuente("ot2_detalle")
        self.assertIn("if (not _es_rol_tecnico() and _ot_aut_es_superadmin()) else None", f)
        self.assertIn("{% if fin_clasif and fin_clasif.texto and not es_tecnico %}", _leer("templates/ot2/detalle.html"))

    def test_excel(self):
        f = _fuente("ot2_reporte_xlsx")
        self.assertIn("_xl_sa = (not _es_rol_tecnico()) and _ot_aut_es_superadmin()", f)
        self.assertIn('if _xl_sa and por_clasif:', f)


class TestClasificacionCalculada(unittest.TestCase):
    """Las OT anteriores al 12-10 se calculan con lo que ya tienen (y lo dicen): nunca se inventa un antecedente."""

    def test_guardada_manda_y_anterior_se_calcula(self):
        guardada = json.dumps({"codigo": "documento_antiguo", "texto": "Documento de más de 3 meses · autorizada"})

        def fetchall(sql, p=None):
            if "FROM mant_visitas" in sql:
                return [{"id": 1, "cliente_id": 3, "modalidad_cobro": None, "factura_tido": "FCV", "valor_origen": "zz",
                         "zz_motivo_manual": None, "fin_clasif_json": guardada},
                        {"id": 2, "cliente_id": 3, "modalidad_cobro": None, "factura_tido": None, "valor_origen": None,
                         "zz_motivo_manual": None, "fin_clasif_json": None},
                        {"id": 3, "cliente_id": 3, "modalidad_cobro": "sin_costo", "factura_tido": None, "valor_origen": None,
                         "zz_motivo_manual": None, "fin_clasif_json": None},
                        {"id": 4, "cliente_id": 3, "modalidad_cobro": None, "factura_tido": None, "valor_origen": None,
                         "zz_motivo_manual": None, "fin_clasif_json": None}]
            if "mant_visita_documentos" in sql:
                return [{"visita_id": 4, "erp_tido": "NVV"}]
            return []
        f = _exec("_ot_fin_clasif_lote", {"mysql_fetchall": fetchall, "_saldo": S, "json": json, "print": lambda *a, **k: None})
        r = f([1, 2, 3, 4])
        self.assertEqual((r[1]["codigo"], r[1]["derivada"]), ("documento_antiguo", False))
        self.assertEqual((r[2]["texto"], r[2]["derivada"]), ("Sin documento", True))   # no dice «autorizada»
        self.assertEqual(r[3]["texto"], "$0 · regalía o arriendo")
        self.assertEqual(r[4]["codigo"], "nota_venta")


# ───────────────────────── asistente (funciones reales en Node) ─────────────────────────
def _js_funcion(nombre):
    i = H.index("function %s(" % nombre)
    prof, j = 0, H.index("{", i)
    while True:
        if H[j] == "{":
            prof += 1
        elif H[j] == "}":
            prof -= 1
            if prof == 0:
                return H[i:j + 1]
        j += 1


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestGuiaEnElNavegador(unittest.TestCase):
    FUNCS = ("_o2mFinFacturaDocs", "_o2mFinFacturaActiva", "_o2mFinObservados", "_o2mFinObsFalta", "_o2mFinGuiaPasos",
             "_o2mFinSaldoFacturas", "_o2mFinSaldoExcesos", "_o2mFinFacturaExcedida", "_o2mFinCobroTotal", "_o2mPesos",
             "_o2mFinDocInfoHtml")

    def _correr(self, s, script):
        prog = "\n".join([
            "var window = {}; var document = {getElementById: function(){ return null; }};",
            "function esInterno(){ return false; } function esc(x){ return String(x == null ? '' : x); }",
            "function _o2mLimpiarNumero(v){ return String(v || '').replace(/[^0-9]/g, ''); }",
            "function _o2mFinDocsTodos(){ return S._docs || []; }",
            "var S = %s;" % json.dumps(s)] + [_js_funcion(n) for n in self.FUNCS] + [script])
        f = os.path.join(tempfile.gettempdir(), "o2m_guia.js")
        open(f, "w", encoding="utf-8").write(prog)
        return json.loads(subprocess.run(["node", f], capture_output=True, text=True, check=True).stdout)

    def _estado(self, obs=None, arg=""):
        doc = {"clave": ["FCV", "11439"], "tido": "FCV", "nudo": "11439", "titulo": "FCV 11439",
               "servicio": {"hay_lineas": True, "total": 100000, "usado": 0, "saldo": 100000},
               "despacho": {"hay_lineas": False, "total": 0, "usado": 0, "saldo": 0},
               "doc": {"leido": True, "fecha": "01/06/2026", "productos": {"total": 1, "con_saldo": 1},
                       "observaciones": obs or []}}
        return {"fin_garantia": False, "fin_aut": False, "fin_centro": "sstt", "fin_zz_monto": 100000,
                "fin_zz_envio": None, "fin_costo_proveedor": "60000", "fin_obs_argumento": arg,
                "fin_tomas": {"FCV|11439": {"servicio": 100000, "despacho": 0}}, "_docs": [{"tido": "FCV", "nudo": "11439"}],
                "fin_saldo": {"docs": [doc], "servicio": {"hay": True, "saldo": 100000}, "despacho": {"hay": False, "saldo": 0}}}

    def test_todo_en_orden(self):
        r = self._correr(self._estado(), "console.log(JSON.stringify(_o2mFinGuiaPasos().map(function(x){ return x.e; })));")
        self.assertEqual(r, ["ok", "ok", "ok", "ok", "espera"])   # margen: sin el motor de la cuenta en esta prueba

    def test_documento_viejo_pide_argumento_y_lo_reconoce(self):
        obs = [{"codigo": "antiguo", "texto": "FCV 11439 es del 01/06/2026 (hace 133 días): tiene más de 3 meses.", "dias": 133}]
        r = self._correr(self._estado(obs), "console.log(JSON.stringify([_o2mFinObsFalta(), _o2mFinGuiaPasos()[2].e]));")
        self.assertEqual(r, [True, "aut"])
        r = self._correr(self._estado(obs, "El cliente pidió instalar después por obras en su gimnasio."),
                         "console.log(JSON.stringify([_o2mFinObsFalta(), _o2mFinGuiaPasos()[2].e]));")
        self.assertEqual(r, [False, "arg"])

    def test_garantia_no_pide_el_argumento_del_documento(self):
        obs = [{"codigo": "antiguo", "texto": "x", "dias": 133}]
        s = dict(self._estado(obs), fin_garantia=True)
        self.assertEqual(self._correr(s, "console.log(JSON.stringify(_o2mFinObsFalta()));"), False)

    def test_la_fila_dice_fecha_y_productos(self):
        r = self._correr(self._estado(), "console.log(JSON.stringify(_o2mFinDocInfoHtml(S.fin_saldo.docs[0])));")
        self.assertIn("Emitida 01/06/2026", r)
        self.assertIn("1 de 1 producto con saldo en Random", r)


class TestAsistenteCableado(unittest.TestCase):
    def test_bloquea_sin_argumento_y_lo_envia(self):
        self.assertIn("        if (_o2mFinObsFalta()) return false;", H)
        self.assertIn("{tipo: 'documento_observado', argumento: (S.fin_obs_argumento || '').trim()}", H)
        self.assertIn("if (res.j.codigo === 'DOC_OBSERVADO'){", H)
        self.assertIn("setFinObsArgumento:setFinObsArgumento,", H)

    def test_sin_alert_nativo(self):   # REGLA #1
        i = H.index("function _o2mFinObsErrorCrear(")
        bloque = H[i:H.index("function _pintarFinGuiaDom(")]
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(" " + nativo, bloque.replace("ilusPrompt(", "").replace("ilusAlert(", ""))


if __name__ == "__main__":
    unittest.main()

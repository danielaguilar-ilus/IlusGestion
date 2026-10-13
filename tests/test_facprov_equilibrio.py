"""⚖️ Punto de equilibrio al pagarle al proveedor (Daniel, 12-oct-2026).

Daniel: «Primero evaluamos la factura. Si está repetida, evaluamos cuánto tiene de saldo. Y el límite es el punto de
equilibrio, donde no ganemos ni perdamos por el servicio. Si se pasa de allí, no puedes asignarla sin autorización»,
«el sistema por ninguna razón debe permitir que paguemos dos veces por un servicio» y «es necesario que esté
ingresada la documentación».
Decisiones (12-oct): la autorización es la MISMA de pérdida del cierre (hasta el monto que vio); una OT en $0 pasa si
ese $0 está autorizado; el candado va al asignar a un lote, al solicitar OC, al marcar pagado sin factura y al marcar
pagada (los lotes ya pagados no se tocan).

Qué se vigila:
  · La cuenta pura: cobro con respaldo = lo cobrado − el saldo que otras OT con prioridad ya usaron; el exceso del
    pago sobre eso necesita autorización (1 peso de tolerancia).
  · El motor de la OT con datos de mentira: sin documento no se paga; factura repetida que ya pagó otra OT → el pago
    se frena; con la autorización de Daniel pasa; $0 autorizado pasa; Random caído → no se paga a ciegas.
  · El cableado: los cuatro caminos que meten OT a un lote o lo pagan pasan por el candado; la autorización de pérdida
    usa el punto de equilibrio; la ficha del lote lo muestra y la pantalla nunca usa alert/confirm/prompt nativos.
Sin BD, sin Flask, sin ERP.
Correr con:  py -m unittest tests.test_facprov_equilibrio
"""
import re
import unittest

import ot_saldo_servicio as S
from tests.test_fac_libro_saldo import _exec
from tests.test_ot_saldo_servicio import _fuente, _leer


class TestCuentaPura(unittest.TestCase):
    def test_sin_factura_repetida_el_limite_es_lo_cobrado(self):
        r = S.equilibrio_pago(70000, 120000)
        self.assertTrue(r["ok"])
        self.assertEqual(r["exceso"], 0)
        self.assertFalse(S.equilibrio_pago(130000, 120000)["ok"])
        self.assertEqual(S.equilibrio_pago(130000, 120000)["exceso"], 10000)

    def test_justo_en_el_equilibrio_pasa(self):
        self.assertTrue(S.equilibrio_pago(120000, 120000)["ok"])
        self.assertTrue(S.equilibrio_pago(120001, 120000)["ok"])    # 1 peso de tolerancia por redondeo
        self.assertFalse(S.equilibrio_pago(120002, 120000)["ok"])

    def test_factura_repetida_baja_el_cobro_con_respaldo(self):
        r = S.equilibrio_pago(70000, 120000, reduccion=100000)
        self.assertEqual(r["cobro_efectivo"], 20000)
        self.assertEqual(r["exceso"], 50000)
        self.assertFalse(r["ok"])

    def test_la_autorizacion_vale_hasta_el_monto_que_daniel_vio(self):
        self.assertTrue(S.equilibrio_pago(70000, 120000, 100000, autorizada=50000)["ok"])
        self.assertFalse(S.equilibrio_pago(80000, 120000, 100000, autorizada=50000)["ok"])

    def test_cero_autorizado_pasa(self):
        self.assertTrue(S.equilibrio_pago(90000, 0, exento=True)["ok"])

    def test_reduccion_por_saldo(self):
        self.assertEqual(S.reduccion_por_saldo(100000, 100000, 100000), 100000)   # otra ya usó todo: nada respalda
        self.assertEqual(S.reduccion_por_saldo(100000, 150000, 50000), 0)         # alcanza
        self.assertEqual(S.reduccion_por_saldo(100000, 150000, 80000), 30000)     # alcanza en parte


def _fin(cobra=True, cobre=120000, pagar=70000, cobertura_txt="Se cobra"):
    return {"cobra": cobra, "cobre": {"total": cobre, "hay": cobre > 0}, "a_pagar_proveedor": pagar,
            "cobertura_txt": cobertura_txt, "me_cobraron": {"tecnico": pagar, "despacho": 0}}


def _motor(fin=None, puerta=None, aportes=None, docs=(("FCV", "0000011439"),), cap=None, usos=(), pagados=(),
           autorizada=0, pendiente=None, lectura_falla=False):
    fin = fin or _fin()
    cap = {"servicio": 100000, "despacho": 0} if cap is None else cap
    amb = {
        "mysql_fetchone": lambda *a, **k: {"id": 50, "numero_ot": "OT-2026-00350", "cliente_id": 7},
        "_ot_fin_cols_sql": lambda a="v": "v.zz_monto",
        "_ot_fin_rep_liviano": lambda x: None, "_ot_repuestos_desglose": lambda ids: {},
        "_ot_finanzas": lambda v, rep: fin,
        "_ot_puerta_documento": lambda v, m, superadmin_declara=None: dict({"ok": True, "via": "documento", "falta": None},
                                                                            **(puerta or {})),
        "_fac_perdida_info": lambda vid, f: {"autorizada": autorizada, "autorizada_por": "Daniel" if autorizada else None,
                                             "pendiente_id": pendiente},
        "_fac_aportes_ot": (lambda vid, solo_cache=False: (_ for _ in ()).throw(S.LecturaIncompleta("x"))) if lectura_falla
        else (lambda vid, solo_cache=False: aportes if aportes is not None else {("FCV", "11439", "servicio"): 100000}),
        "_ot_saldo_docs_de_ot": lambda vid: list(docs),
        "_ot_saldo_familia": lambda t, n: {S.clave_doc(t, n)},
        "_OT_SALDO_FAM_CACHE": {},
        "_fac_familia_persistida": lambda k: set(),   # familia desde la foto del libro (caché fría): se prueba aparte
        "_fac_leer_capacidad": lambda t, n, solo_cache=False: ("ok", cap) if cap is not False else ("no_leido", None),
        "_ot_doc_real_a_usuario": lambda t, n: (t, str(n).lstrip("0")),
        "_ot_saldo_usos": lambda fam, excluir_vid=None, cap=None: list(usos),
        "_fac_pagados_a_proveedor": lambda vids: set(pagados),
        "_saldo": S,
    }
    return _exec("_fac_equilibrio_ot", amb)


OTRA = {"vid": 60, "numero_ot": "OT-2026-00360", "cliente": "Gimnasio de ejemplo", "estado": "cerrada",
        "servicio": 100000, "despacho": 0, "clave": ("FCV", "11439")}


class TestMotorDeLaOT(unittest.TestCase):
    def test_dentro_del_equilibrio(self):
        r = _motor()(50)
        self.assertTrue(r["ok"])
        self.assertEqual(r["estado"], "ok")
        self.assertEqual(r["documentos"][0]["titulo"], "FCV 11439")

    def test_varias_facturas_ninguna_es_misma_venta_que_si_misma(self):
        # 🐛 caso real 12-10 (OT-2026-00171, lote 19): FCV 11439, 11440 y 11441 salían «misma venta que» sí mismas.
        r = _motor(docs=(("FCV", "0000011439"), ("FCV", "0000011440"), ("FCV", "0000011441")))(50)
        self.assertEqual([d["titulo"] for d in r["documentos"]], ["FCV 0000011439", "FCV 0000011440", "FCV 0000011441"]
                         if r["documentos"][0]["titulo"].startswith("FCV 0") else ["FCV 11439", "FCV 11440", "FCV 11441"])
        self.assertEqual([d["herencia"] for d in r["documentos"]], [[], [], []])

    def test_sin_documento_se_paga_igual_y_avisa(self):
        # Daniel 12-oct: «no es culpa del proveedor si tiene o no factura»; el documento se exige al crear la OT.
        r = _motor(puerta={"ok": False, "via": None, "falta": "documento"})(50)
        self.assertTrue(r["ok"])
        self.assertEqual(r["estado"], "sin_documento")
        self.assertIn("no es su responsabilidad", r["mensaje"])

    def test_con_nota_de_venta_si_se_paga(self):
        r = _motor(puerta={"ok": False, "via": None, "falta": "factura"})(50)
        self.assertTrue(r["ok"])
        self.assertTrue(r["nota_venta"])

    def test_factura_repetida_que_ya_pago_otra_ot_frena_el_pago(self):
        # La OT-360 (posterior) ya se le pagó al proveedor con esa factura: tiene prioridad sobre el saldo.
        r = _motor(usos=[OTRA], pagados={60})(50)
        self.assertFalse(r["ok"])
        self.assertEqual(r["code"], "SOBRE_EQUILIBRIO")
        self.assertTrue(r["compartida"])
        self.assertEqual(r["reduccion"], 100000)
        self.assertIn("ya los usaron otras OT", r["mensaje"])
        self.assertEqual(r["documentos"][0]["otras"][0]["numero_ot"], "OT-2026-00360")

    def test_factura_repetida_en_una_ot_posterior_sin_pagar_no_frena_a_la_primera(self):
        r = _motor(usos=[OTRA], pagados=set())(50)       # la OT-360 es posterior y no está pagada
        self.assertTrue(r["ok"])
        self.assertTrue(r["compartida"])

    def test_con_la_autorizacion_de_daniel_pasa(self):
        r = _motor(usos=[OTRA], pagados={60}, autorizada=50000)(50)
        self.assertTrue(r["ok"])
        self.assertEqual(r["estado"], "autorizada")

    def test_cero_autorizado_pasa_y_sin_autorizar_no(self):
        f0 = _fin(cobra=False, cobre=0, pagar=90000, cobertura_txt="Garantía")
        self.assertEqual(_motor(fin=f0, puerta={"ok": True, "via": "cobro_cero"})(50)["estado"], "exento")
        r = _motor(fin=f0, puerta={"ok": False, "via": None, "falta": "autorizacion_cobro_cero"})(50)
        self.assertEqual(r["code"], "SIN_AUTORIZACION_CERO")
        self.assertTrue(r["ok"])                     # tampoco es culpa del proveedor: se avisa, no frena

    def test_random_caido_no_paga_a_ciegas(self):
        r = _motor(lectura_falla=True)(50)
        self.assertFalse(r["ok"])
        self.assertEqual(r["code"], "LECTURA_INCOMPLETA")
        r = _motor(cap=False)(50)
        self.assertEqual(r["code"], "LECTURA_INCOMPLETA")

    def test_en_listas_random_caido_solo_avisa(self):
        r = _motor(cap=False)(50, solo_cache=True)
        self.assertTrue(r["ok"])
        self.assertEqual(r["estado"], "sin_verificar")


class TestMargenConRespaldo(unittest.TestCase):
    """Daniel: «no nos podemos equivocar para tener los costos controlados, es mi promesa de pago». El margen del lote
    (fila, totales, KPI y Excel) usa lo cobrado CON RESPALDO de la factura, no lo declarado."""

    def _resp(self, it):
        return _exec("_mfp_cobro_respaldado", {})(it)

    def test_factura_ya_usada_por_otra_ot_no_cuenta_como_cobrado(self):
        it = {"cobrado_cliente": 85000, "eq": {"estado": "sobre_equilibrio", "cobro_efectivo": 0}}
        self.assertEqual(self._resp(it), 0)

    def test_dentro_del_equilibrio_cuenta_lo_declarado(self):
        it = {"cobrado_cliente": 120000, "eq": {"estado": "ok", "cobro_efectivo": 120000}}
        self.assertEqual(self._resp(it), 120000)

    def test_sin_documento_no_hay_cobro_respaldado(self):
        self.assertEqual(self._resp({"cobrado_cliente": 90000, "eq": {"estado": "sin_documento"}}), 0)

    def test_sin_verificar_queda_lo_declarado(self):
        self.assertEqual(self._resp({"cobrado_cliente": 90000, "eq": {"estado": "sin_verificar"}}), 90000)

    def test_los_totales_del_lote_suman_lo_respaldado(self):
        tot = _exec("_mfp_totales_lote", {})(190000, [
            {"sugerido": 70000, "fac_monto": None, "cobrado_cliente": 120000, "cobrado_respaldado": 120000},
            {"sugerido": 60000, "fac_monto": None, "cobrado_cliente": 85000, "cobrado_respaldado": 0}])
        self.assertEqual(tot["total_cobrado_cliente"], 120000)
        self.assertEqual(tot["total_cobrado_declarado"], 205000)
        self.assertEqual(tot["margen_filas"], 120000 - 130000)        # pierde: la segunda OT no tiene respaldo

    def test_fila_y_excel_usan_la_misma_cuenta(self):
        it = _fuente("_mfp_items")
        self.assertIn('it["cobrado_respaldado"] = _mfp_cobro_respaldado(it)', it)
        self.assertIn('it["margen_fila"] = it["cobrado_respaldado"] - _base', it)
        x = _fuente("_mfp_excel_lote")
        self.assertIn('"Cobrado con respaldo de su factura"', x)
        self.assertIn("f\"=SUM({rng('V')})\"", x)
        t = _leer("templates/mantenciones/factura_proveedor_detalle.html")
        self.assertIn("con respaldo de su factura", t)


class TestDesgloseDelDocumento(unittest.TestCase):
    def test_productos_servicio_y_despacho_separados(self):
        d = S.desglose_documento({"monto": 1500000, "lineas": 3}, {"total": 120000}, {"total": 30000})
        self.assertEqual((d["productos"], d["servicio"], d["despacho"]), (1500000, 120000, 30000))
        self.assertIn("no se cobran en la OT", d["texto"])

    def test_el_asistente_y_la_ficha_lo_muestran(self):
        self.assertIn("_o2mDesgloseHtml(av.desglose)", _leer("templates/ot2/_modal_crear.html"))
        self.assertIn("av.desglose.texto", _leer("static/ot_fin_motor.js"))
        self.assertIn("_saldo.desglose_documento(", _fuente("_ot_saldo_aviso"))


class TestSoloEquipoInterno(unittest.TestCase):
    """Daniel 12-oct: «los costos solo se pueden ver desde el equipo interno; excluye a los técnicos, tanto internos
    como externos» (REGLA #19 y #26). Doble candado en cada camino: decorador y permiso que nunca deja pasar a un técnico."""

    def test_rutas_del_lote_y_del_pago_cierran_a_los_tecnicos(self):
        from tests.test_ot_saldo_servicio import _decoradores
        for f in ("mant_factura_proveedor_detalle", "mant_factura_proveedor_excel", "mant_factura_proveedor_asignar",
                  "mant_factura_proveedor_pagar", "mant_facturas_proveedor_solicitar_oc",
                  "mant_facturas_proveedor_marcar_pagado_sin_factura", "ot_aut_api_crear"):
            self.assertIn("_no_tecnico", _decoradores(f), f)
        self.assertIn("if _es_rol_tecnico(u):", _fuente("_facprov_puede"))

    def test_el_desglose_y_el_saldo_no_se_entregan_a_un_tecnico(self):
        src = _fuente("ot_api_saldo_servicio_doc")
        self.assertIn("if _es_rol_tecnico():", src)
        self.assertLess(src.index("if _es_rol_tecnico():"), src.index("_ot_saldo_doc("))


class TestCableado(unittest.TestCase):
    def test_los_caminos_que_pagan_pasan_por_el_candado(self):
        for f in ("mant_factura_proveedor_asignar", "mant_factura_proveedor_pagar",
                  "mant_facturas_proveedor_solicitar_oc", "mant_facturas_proveedor_marcar_pagado_sin_factura"):
            src = _fuente(f)
            self.assertIn("_fac_equilibrio_rechazo(", src, f)
            self.assertIn("_fac_equilibrio_ot(", src, f)
        # el candado va ANTES de escribir
        a = _fuente("mant_factura_proveedor_asignar")
        self.assertLess(a.index("_fac_equilibrio_rechazo("), a.index("INSERT INTO mant_factura_proveedor_items"))
        p = _fuente("mant_factura_proveedor_pagar")
        self.assertLess(p.index("_fac_equilibrio_rechazo("), p.index("UPDATE mant_facturas_proveedor SET estado_pago='pagada'"))

    def test_la_autorizacion_de_perdida_usa_el_equilibrio(self):
        src = _fuente("ot_aut_api_crear")
        self.assertIn("_fac_equilibrio_ot(visita_id", src)
        self.assertIn("pago_proveedor", src)

    def test_la_ficha_del_lote_lo_muestra(self):
        self.assertIn('_fac_equilibrio_ot(it["id"], pago=_base, solo_cache=True)', _fuente("_mfp_items"))
        self.assertIn('eq_sobre = sum(1 for it in items if it.get("eq") and not it["eq"].get("ok"))', _fuente("mant_factura_proveedor_detalle"))
        t = _leer("templates/mantenciones/factura_proveedor_detalle.html")
        self.assertIn('class="fpd-eq fpd-eq-{{ eq.estado }}"', t)
        self.assertIn("function fpdPedirAutEquilibrio(", t)
        self.assertIn("tipo: 'cerrar_con_perdida'", t)
        i = t.index("function fpdEsCandadoEquilibrio")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", t[i:t.index("async function fpdQuitar", i)]))


if __name__ == "__main__":
    unittest.main()

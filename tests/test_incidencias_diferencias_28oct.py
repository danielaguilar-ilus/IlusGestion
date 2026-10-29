# -*- coding: utf-8 -*-
"""Bodega de incidencias → «Revisar diferencias» (Daniel, 28-oct-2026): buscador, «Registrar» la unidad que falta aunque el SKU ya tenga
incidencia, y modal ancho.

Caso real: la ILUS Air Ski Erg Machine (SKU 1050100999). El ERP dice 2, Check tiene 2 UA, nosotros tenemos 1 incidencia (UA1014361). La fila solo
decía «Ver / corregir» y no dejaba agregar la otra máquina. Ahora la fila trae cuántas faltan y con qué UA libre se registra.
Las funciones de app.py corren sobre una base y un WMS de mentira (tests/_fuente_app.py)."""
import json
import os
import re
import shutil
import subprocess
import unittest

from tests._fuente_app import cargar, decoradores, fuente_funcion, texto

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANTILLA = os.path.join(RAIZ, "templates", "mantenciones", "incidencias.html")


class _IncConcMysqlError(Exception):
    pass


def _wms(ua, ubicacion, sku="1050100999", desc="ILUS Air Ski Erg Machine", cant=1):
    return {"bodega": "BODEGA 13", "ua": ua, "codigo": sku, "descripcion": desc, "stFisico": cant, "ubicacion": ubicacion}


def _hallazgos(wms, incs, erp):
    amb = cargar(
        ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_consolidar_por_sku", "_inc_pendiente_dividir", "_inc_calcular_hallazgos"),
        ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
        extra={
            "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda forzar=False: wms,
            "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda forzar=False: erp,
            "_checkwms_norm_ua": lambda s: (s or "").strip().upper(),
            "_inc_clasificacion_skus_batch": lambda skus: {}, "_inc_hallazgo_falta_registrar": lambda ua, r, e, c: {"tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo")},
            "_inc_baja_clave": lambda h: "%s|%s" % (h.get("tipo"), h.get("sku")), "_inc_completar_nombres": lambda hs: None,
        })
    return amb["_inc_calcular_hallazgos"]()["hallazgos"]


class TestRegistrarLaUnidadQueFalta(unittest.TestCase):
    INC = [{"id": 7, "sku": "1050100999", "descripcion": "ILUS Air Ski Erg Machine", "cantidad": 1,
            "motivo": "Falla de consola que no enciende", "estado": "abierta", "recomendacion": "UA1014361"}]

    def test_air_ski_erg_trae_cuantas_faltan_y_con_que_ua_se_registra(self):
        wms = [_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3")]
        hs = _hallazgos(wms, self.INC, {"1050100999": 2})
        h = next(x for x in hs if x["tipo"] == "dif_erp")
        self.assertEqual(h["accion"], "ver")                       # sigue pudiendo corregir la incidencia que ya existe
        self.assertEqual(h["faltan"], 1)
        self.assertTrue(h["puede_registrar"])
        self.assertEqual(h["ua"], "UA1014362")                     # NO la UA que ya tiene incidencia (la UA es única por incidencia)
        self.assertEqual(h["ubicacion"], "B-02-3")
        self.assertEqual(h["uas_libres"], [{"ua": "UA1014362", "ubicacion": "B-02-3"}])
        self.assertEqual(h["cantidad"], 1)

    def test_con_varias_ua_libres_se_ofrecen_todas(self):
        wms = [_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3"), _wms("UA1014363", "B-02-4")]
        h = next(x for x in _hallazgos(wms, self.INC, {"1050100999": 3}) if x["tipo"] == "dif_erp")
        self.assertEqual([u["ua"] for u in h["uas_libres"]], ["UA1014362", "UA1014363"])
        self.assertEqual(h["faltan"], 2)

    def test_diferencia_con_check_tambien_se_puede_registrar(self):
        wms = [_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3")]
        hs = _hallazgos(wms, self.INC, {"1050100999": 1})          # el ERP cuadra con nosotros; Check tiene 2
        h = next(x for x in hs if x["tipo"] == "dif_wms")
        self.assertEqual((h["faltan"], h["puede_registrar"], h["ua"]), (1, True, "UA1014362"))
        self.assertEqual(h["cantidad"], 1)

    def test_si_declaramos_de_mas_no_hay_nada_que_registrar(self):
        inc2 = self.INC + [dict(self.INC[0], id=8, recomendacion="UA1014362")]
        hs = _hallazgos([_wms("UA1014361", "A-01-1")], inc2, {"1050100999": 1})
        h = next(x for x in hs if x["tipo"] in ("dif_erp", "dif_wms"))
        self.assertEqual((h["faltan"], h["puede_registrar"]), (0, False))

    def test_sin_otra_ua_libre_no_se_inventa_una(self):
        # 2 unidades en la MISMA UA (Check): la única UA ya tiene incidencia → no hay UA libre que proponer
        hs = _hallazgos([_wms("UA1014361", "A-01-1", cant=2)], self.INC, {"1050100999": 2})
        h = next(x for x in hs if x["tipo"] in ("dif_erp", "dif_wms"))
        self.assertEqual(h["uas_libres"], [])
        self.assertTrue(h["puede_registrar"])                       # el front explica qué hacer en vez de proponer una UA en uso

    def test_un_sku_sin_incidencia_sigue_siendo_registrar_como_antes(self):
        hs = _hallazgos([_wms("UA1014361", "A-01-1")], [], {"1050100999": 1})
        h = next(x for x in hs if x["tipo"] == "dif_erp")
        self.assertEqual((h["accion"], h["faltan"], h["puede_registrar"]), ("registrar", 0, False))   # sin ids: ya era «Registrar»


class TestGestionarLaDiferenciaUnidadPorUnidad(unittest.TestCase):
    """Daniel: «90 pisos van en una línea, pero 2 máquinas van separadas porque cada una tiene un motivo distinto. Al abrir la diferencia que me diga:
    esta tiene motivo, esta tiene motivo, falta agregar uno»."""
    INC = TestRegistrarLaUnidadQueFalta.INC

    def _dif(self, wms, incs, erp, repetible=False, tipo="dif_erp"):
        amb = cargar(
            ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_consolidar_por_sku", "_inc_pendiente_dividir", "_inc_calcular_hallazgos"),
            ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
            extra={
                "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda forzar=False: wms,
                "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda forzar=False: erp,
                "_checkwms_norm_ua": lambda s: (s or "").strip().upper(),
                "_inc_clasificacion_skus_batch": lambda skus: {s: {"repetible": repetible} for s in skus},
                "_inc_hallazgo_falta_registrar": lambda ua, r, e, c: {"tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo")},
                "_inc_baja_clave": lambda h: "k", "_inc_completar_nombres": lambda hs: None,
            })
        hs = amb["_inc_calcular_hallazgos"]()["hallazgos"]
        return next(x for x in hs if x["tipo"] == tipo)

    def test_air_ski_erg_una_con_motivo_y_otra_que_falta_agregar(self):
        h = self._dif([_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3")], self.INC, {"1050100999": 2})
        u = h["unidades"]
        self.assertEqual([(c["clase"], c.get("ua"), c.get("con_motivo")) for c in u["tarjetas"]],
                         [("incidencia", "UA1014361", True), ("libre", "UA1014362", None)])
        self.assertEqual((u["unidades"], u["con_motivo"], u["faltan"], u["sobran"]), (2, 1, 1, 0))
        self.assertFalse(h["repetible"])
        self.assertEqual(u["tarjetas"][0]["motivo"], "Falla de consola que no enciende")      # el motivo se ve completo en la tarjeta

    def test_un_motivo_muy_corto_se_marca_como_falta_el_motivo(self):
        inc = [dict(self.INC[0], motivo="roto")]
        u = self._dif([_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3")], inc, {"1050100999": 2})["unidades"]
        self.assertFalse(u["tarjetas"][0]["con_motivo"])
        self.assertEqual(u["con_motivo"], 0)

    def test_un_equipo_trae_una_tarjeta_por_cada_ua_libre(self):
        wms = [_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3"), _wms("UA1014363", "B-02-4")]
        u = self._dif(wms, self.INC, {"1050100999": 3})["unidades"]
        self.assertEqual([c["clase"] for c in u["tarjetas"]], ["incidencia", "libre", "libre"])
        self.assertEqual(u["faltan"], 2)

    def test_un_producto_repetible_va_en_una_sola_linea_con_la_cantidad(self):
        inc = [dict(self.INC[0], cantidad=10, recomendacion="UA1")]
        wms = [_wms("UA1", "P-1", cant=10), _wms("UA2", "P-2", cant=80)]
        h = self._dif(wms, inc, {"1050100999": 90}, repetible=True)
        t = h["unidades"]["tarjetas"]
        self.assertEqual([c["clase"] for c in t], ["incidencia", "faltan_repetible"])
        self.assertEqual(t[1]["cantidad"], 80)                                                  # 80 pisos juntos, NO 80 tarjetas
        self.assertTrue(h["repetible"])

    def test_sin_otra_ua_libre_se_dice_que_hay_que_separarlas_en_check(self):
        u = self._dif([_wms("UA1014361", "A-01-1", cant=2)], self.INC, {"1050100999": 2})["unidades"]
        self.assertEqual([c["clase"] for c in u["tarjetas"]], ["incidencia", "sin_ua"])
        self.assertEqual(u["tarjetas"][1]["cantidad"], 1)

    def test_si_declaramos_de_mas_lo_dice(self):
        inc2 = self.INC + [dict(self.INC[0], id=8, recomendacion="UA1014362")]
        u = self._dif([_wms("UA1014361", "A-01-1")], inc2, {"1050100999": 1})["unidades"]
        self.assertEqual((u["faltan"], u["sobran"]), (0, 1))

    def test_la_diferencia_con_check_tambien_trae_su_detalle(self):
        h = self._dif([_wms("UA1014361", "A-01-1"), _wms("UA1014362", "B-02-3")], self.INC, {"1050100999": 1}, tipo="dif_wms")
        self.assertEqual([c["clase"] for c in h["unidades"]["tarjetas"]], ["incidencia", "libre"])

    def test_la_tabla_abre_el_panel_y_las_tarjetas_reusan_las_acciones_que_ya_existen(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn('id="incDifModal"', t)
        self.assertIn("incAbrirDiferencia(h); return;", t)
        panel = t.split("function incAbrirDiferencia")[1][:9500]
        for ancla in ("incAbrirPorId(c.id)", "incAbrirBaja(", "incRegistrarConPregunta(Object.assign", "con motivo", "Falta agregar", "Registrar esta UA"):
            self.assertIn(ancla, panel, ancla)
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", panel))
        # «Ver / corregir» no se pierde: queda en el menú de la fila (REGLA #4.2)
        self.assertIn("Ver / corregir la incidencia", t)


class TestUnaFilaPorSku(unittest.TestCase):
    """Daniel (28-oct), con la Air Ski Erg: «dice que hay tres diferencias, es confuso». Era la MISMA máquina contada por tres caminos (diferencia con el
    ERP, diferencia con Check y «Falta registrar» de su UA) y la UA salía como «UA UA1014361». Ahora es UNA fila con las unidades por UA y motivo."""
    WMS = [_wms("UA1013428", "ARMADO01"), _wms("UA1014361", "ARMADO01")]
    INC = [{"id": 7, "sku": "1050100999", "descripcion": "ILUS Air Ski Erg Machine", "cantidad": 1,
            "motivo": "Falla de consola que no enciende", "estado": "abierta", "recomendacion": "UA1013428"}]

    def _todos(self, wms, incs, erp, falta_registrar=None):
        amb = cargar(
            ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_consolidar_por_sku",
             "_inc_pendiente_dividir", "_inc_calcular_hallazgos"),
            ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
            extra={
                "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda forzar=False: wms,
                "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda forzar=False: erp,
                "_checkwms_norm_ua": lambda s: (s or "").strip().upper(),
                "_inc_clasificacion_skus_batch": lambda skus: {s: {"repetible": False} for s in skus},
                "_inc_hallazgo_falta_registrar": falta_registrar or (lambda ua, r, e, c: {
                    "tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo"), "wms": 1, "nuestra_bd": 0, "sugerir_distribuir": False}),
                "_inc_baja_clave": lambda h: "k", "_inc_completar_nombres": lambda hs: None,
            })
        return amb["_inc_calcular_hallazgos"]()["hallazgos"]

    def test_la_misma_maquina_ya_no_sale_tres_veces(self):
        hs = [h for h in self._todos(self.WMS, self.INC, {"1050100999": 2}) if h.get("sku") == "1050100999"]
        self.assertEqual([h["tipo"] for h in hs], ["dif_erp"])                      # antes: dif_erp + dif_wms + falta_registrar
        h = hs[0]
        self.assertTrue(h["tambien_wms"])                                          # la diferencia con Check se sigue contando
        self.assertEqual([(c["clase"], c["ua"]) for c in h["unidades"]["tarjetas"]], [("incidencia", "UA1013428"), ("libre", "UA1014361")])

    def test_si_solo_difiere_con_check_queda_esa_fila(self):
        hs = self._todos(self.WMS, self.INC, {"1050100999": 1})
        self.assertEqual([h["tipo"] for h in hs if h.get("sku") == "1050100999"], ["dif_wms"])

    def test_cada_sku_conserva_su_propia_fila_y_su_ua_libre_sale_como_tarjeta(self):
        wms = self.WMS + [_wms("UA9", "X-1", sku="2000", desc="Otro")]
        hs = self._todos(wms, self.INC, {"1050100999": 2, "2000": 1})
        otro = [h for h in hs if h.get("sku") == "2000"]
        self.assertEqual([h["tipo"] for h in otro], ["dif_erp"])                              # una fila por SKU, también el otro
        self.assertEqual([(c["clase"], c["ua"]) for c in otro[0]["unidades"]["tarjetas"]], [("libre", "UA9")])   # su «Falta registrar» quedó adentro

    def test_pendiente_dividir_cuando_faltan_unidades_y_no_hay_otra_ua_libre(self):
        hs = self._todos([_wms("UA1013428", "ARMADO01", cant=2)], self.INC, {"1050100999": 2})
        h = next(x for x in hs if x.get("sku") == "1050100999")
        self.assertTrue(h["pendiente_dividir"])

    def test_no_es_pendiente_dividir_si_hay_una_ua_por_unidad(self):
        h = next(x for x in self._todos(self.WMS, self.INC, {"1050100999": 2}) if x.get("sku") == "1050100999")
        self.assertFalse(h["pendiente_dividir"])

    def test_varias_unidades_en_una_ua_sin_incidencia_tambien_se_marcan(self):
        fr = lambda ua, r, e, c: {"tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo"), "wms": 3, "nuestra_bd": 0, "sugerir_distribuir": True}
        wms = [_wms("UA5", "Z-1", sku="3000", desc="Remo", cant=3)]
        hs = [x for x in self._todos(wms, [], {}, falta_registrar=fr) if x.get("sku") == "3000"]
        self.assertNotIn("falta_registrar", [x["tipo"] for x in hs])                  # su UA quedó adentro de la diferencia con Check
        dif = next(x for x in hs if x["tipo"] == "dif_wms")
        self.assertTrue(dif["pendiente_dividir"])                                      # 3 unidades en una sola UA: pendiente de dividir en Check
        self.assertEqual([c["clase"] for c in dif["unidades"]["tarjetas"]], ["libre", "sin_ua"])

    def test_el_front_muestra_la_ua_una_sola_vez_y_las_unidades_por_ua_y_motivo(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertNotIn("'<div class=\"rm-sub\">UA '+incEsc(h.ua)+'</div>'", t)            # el origen del «UA UA1014361»
        self.assertIn("function incUaTxt(ua)", t)
        self.assertIn("/^UA/i.test(u)", t)
        self.assertIn("incUnidadesChips(h.unidades.tarjetas)", t)
        self.assertIn("Por UA y motivo", t)
        self.assertIn("Pendiente dividir en UA en Check", t)
        self.assertIn("['dividir',         'k-div', 'Dividir UA en Check'", t)
        self.assertIn("if(incConcFiltro === 'dividir') return !!h.pendiente_dividir;", t)

    def test_la_logica_de_la_etiqueta_ua(self):
        exe = shutil.which("node")
        if not exe:
            self.skipTest("Node no está instalado")
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        f = re.search(r"function incUaTxt\(ua\)\{.*?\n  \}", t, re.S).group(0)
        prog = ("const incEsc=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;');" + f +
                "process.stdout.write(JSON.stringify([incUaTxt('UA1014361'),incUaTxt('ua1014361'),incUaTxt('1014361'),incUaTxt(''),incUaTxt(null)]));")
        r = subprocess.run([exe, "-e", prog], capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), ["UA1014361", "ua1014361", "UA 1014361", "", ""])


class TestActualizarYPorQueNoAparece(unittest.TestCase):
    """Daniel (28-oct): «se hizo un traslado hoy a la bodega 13 en el ERP y no salen las diferencias… yo quiero saber». El ERP se leía con caché de 15 min y
    Check de 1 hora, y «Actualizar» no las saltaba."""

    def test_actualizar_salta_las_caches_y_abrir_el_panel_no(self):
        src = texto("app.py")
        self.assertIn("def _inc_calcular_hallazgos(forzar=False):", src)
        self.assertIn("_checkwms_stock_rows(forzar=forzar)", src)
        self.assertIn("_erp_stock_b13_por_sku(forzar=forzar)", src)
        self.assertIn('_inc_calcular_hallazgos(forzar=(request.args.get("forzar") == "1"))', src)
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn("incConcCargar(true)", t)                                    # solo el botón «Actualizar»
        self.assertIn("'?forzar=1'", t)
        self.assertIn("$('incBtnConciliar').addEventListener('click', function(){ incConcCargar(); });", t)

    def _v(self, erp13, chk13, nuestra, otras=()):
        return cargar(("_inc_veredicto_producto",))["_inc_veredicto_producto"](erp13, chk13, nuestra, otras)

    def test_el_traslado_aun_no_recepcionado_se_explica(self):
        txt = self._v(0, 0, 0, [("05", 1.0)])
        self.assertIn("no tiene stock de este producto en la bodega 13", txt)
        self.assertIn("05: 1", txt)
        self.assertIn("no cuentan para esta", txt)                  # el stock de otras bodegas NO entra en la comparación
        self.assertIn("recepcionado", txt)

    def test_el_caso_de_la_rower_erp_si_check_no_y_nosotros_no(self):
        # Daniel (29-oct): «si en la bodega 13 está y en Check no está, hay que hacer un ingreso en Check; y si en nuestra bodega tampoco está, hay que hacer
        # un ingreso y declarar motivo, pedir repuesto y toda la gestión»
        txt = self._v(1, 0, 0, [("008", 6.0), ("02", 11.0)])
        self.assertIn("Bodega 13: ERP 1, Check 0, nosotros 0.", txt)
        self.assertIn("ingreso en Check de 1 unidad", txt)
        self.assertIn("registrar 1 en incidencias, con su motivo (y pedir repuesto", txt)
        self.assertIn("Actualizar la lista y buscar este SKU", txt)

    def test_si_check_la_tiene_y_nosotros_no_solo_falta_registrar(self):
        txt = self._v(1, 1, 0)
        self.assertNotIn("ingreso en Check", txt)
        self.assertIn("registrar 1 en incidencias", txt)

    def test_si_cuadra_lo_dice(self):
        self.assertTrue(self._v(1, 1, 1).startswith("Cuadra en la bodega 13"))

    def test_si_declaramos_de_mas(self):
        self.assertIn("declaramos 1 de más", self._v(1, 1, 2))

    def test_la_consulta_en_vivo_es_solo_lectura_y_cerrada_a_tecnicos(self):
        src = fuente_funcion("mant_api_incidencias_diagnostico")
        self.assertNotIn("INSERT", src.upper().replace("INSERT_", ""))
        self.assertNotIn("UPDATE", src.upper())
        self.assertIn("_checkwms_stock_rows(forzar=True)", src)
        self.assertIn("_random_sql_query(", src)
        self.assertIn("FROM MAEST", src)
        decs = decoradores("mant_api_incidencias_diagnostico")
        self.assertTrue(any("_no_tecnico_salvo_taller" in d for d in decs))
        self.assertIn("len(q) < 4", src)                                          # no se puede pedir «todo»
        # 29-oct: busca también por NOMBRE (antes «rower» devolvía «no aparece en ningún lado»), y solo la bodega 13 cuenta
        self.assertIn("NOKOPR LIKE %s", src)
        self.assertIn("r.bodega == INC_BODEGA_ERP" if False else 'r["bodega"] == INC_BODEGA_ERP', src)
        self.assertIn("UPPER(descripcion) LIKE %s", src)


class TestConsultaPorDocumento(unittest.TestCase):
    """Daniel (28-oct): «se hizo un traslado hoy a la bodega 13 en el ERP… está bajo la GRI 2721, el SKU termina en 101054… yo quiero saber» →
    «bien inteligente la consulta»: entiende un documento del ERP y lo cruza con el stock por bodega y con Check."""

    def _f(self):
        return cargar(("_inc_doc_ref", "_inc_doc_texto", "_inc_veredicto_producto"))

    def test_reconoce_la_referencia_de_un_documento(self):
        f = self._f()["_inc_doc_ref"]
        self.assertEqual(f("GRI2721"), ("GRI", "2721"))
        self.assertEqual(f("GRI0000002721"), ("GRI", "2721"))
        self.assertIsNone(f("1049101054"))                    # un SKU numérico no es un documento
        self.assertIsNone(f("UA1014361"))                     # una UA tampoco (2 letras)
        self.assertIsNone(f(""))

    def test_documento_que_existe_con_la_linea_del_sku(self):
        docs = [{"tido": "GRI", "nudo": "0000002721", "fecha": "28/10/2026", "estado": "", "lineas": [
            {"sku": "1049101054", "cant": 1.0, "cant_despachada": 0.0, "bodegas": {"BOSULIDO": "05", "BOENLIDO": "13"}},
            {"sku": "9999", "cant": 2.0, "cant_despachada": 0.0, "bodegas": {}}]}]
        t = self._f()["_inc_doc_texto"](docs, ("GRI", "2721"), "101054")
        self.assertIn("SÍ tiene el documento GRI 2721 del 28/10/2026", t)
        self.assertIn("1049101054 × 1", t)
        self.assertIn("BOENLIDO=13", t)
        self.assertNotIn("9999", t)                            # solo la línea del producto que se pregunta

    def test_documento_anulado_o_que_no_existe_o_sin_el_producto(self):
        f = self._f()["_inc_doc_texto"]
        self.assertIn("ANULADO", f([{"tido": "GRI", "nudo": "2721", "fecha": "", "estado": "NULO", "lineas": []}], ("GRI", "2721")))
        self.assertIn("No encontré el documento GRI 2721", f([], ("GRI", "2721")))
        t = f([{"tido": "GRI", "nudo": "2721", "fecha": "", "estado": "", "lineas": [{"sku": "ABC", "cant": 1.0, "bodegas": {}}]}], ("GRI", "2721"), "101054")
        self.assertIn("Ninguna de sus 1 línea(s)", t)

    def test_el_endpoint_busca_el_documento_solo_en_lectura(self):
        src = fuente_funcion("mant_api_incidencias_diagnostico")
        self.assertIn("_inc_doc_ref(q)", src)
        self.assertIn("FROM MAEEDO WHERE TIDO=%s AND NUDO LIKE %s", src)
        self.assertIn("SELECT TOP 40 * FROM MAEDDO WHERE IDMAEEDO=%s", src)
        for prohibido in ("INSERT", "UPDATE", "DELETE", "EXEC"):
            self.assertNotIn(prohibido, src.upper().replace("INSERT_", ""), prohibido)
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn("GRI 2721", t)                                              # el tip visible en la pantalla


class TestFaltaIngresoEnCheck(unittest.TestCase):
    """Daniel (29-oct), ILUS Rower Pro (SKU 1049101054): el ERP la tiene en la bodega 13 (1 unidad) y Check no. Antes un SKU que Check no tenía se saltaba en
    silencio y no salía ninguna diferencia."""
    STOCK_VACIO = [_wms("UA9", "X-1", sku="2000", desc="Otro")]            # Check solo tiene OTRO producto
    SKU = "1049101054"

    def _todos(self, wms, incs, erp):
        amb = cargar(
            ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_consolidar_por_sku",
             "_inc_pendiente_dividir", "_inc_calcular_hallazgos"),
            ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
            extra={
                "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda forzar=False: wms,
                "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda forzar=False: erp,
                "_checkwms_norm_ua": lambda s: (s or "").strip().upper(),
                "_inc_clasificacion_skus_batch": lambda skus: {s: {"repetible": False} for s in skus},
                "_inc_hallazgo_falta_registrar": lambda ua, r, e, c: {"tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo")},
                "_inc_baja_clave": lambda h: "k", "_inc_completar_nombres": lambda hs: None,
            })
        return amb["_inc_calcular_hallazgos"]()["hallazgos"]

    def test_el_erp_la_tiene_y_nadie_mas_aparece_la_diferencia_con_su_tarjeta(self):
        hs = [h for h in self._todos(self.STOCK_VACIO, [], {self.SKU: 1, "2000": 1}) if h.get("sku") == self.SKU]
        self.assertEqual([h["tipo"] for h in hs], ["dif_erp"])
        h = hs[0]
        self.assertTrue(h["falta_ingreso_check"])
        self.assertEqual(h["unidades"]["sin_check"], 1)
        self.assertEqual([(c["clase"], c["cantidad"]) for c in h["unidades"]["tarjetas"]], [("falta_check", 1)])
        self.assertIn("hay que hacer el ingreso en Check", h["detalle"])
        self.assertEqual(h["accion"], "registrar")                           # se puede registrar ya (sin UA todavía)

    def test_si_ya_la_declaramos_sin_ua_igual_sale_porque_check_no_la_tiene(self):
        inc = [{"id": 5, "sku": self.SKU, "descripcion": "ILUS Rower Pro", "cantidad": 1, "motivo": "Falta el asiento completo", "estado": "abierta", "recomendacion": ""}]
        hs = [h for h in self._todos(self.STOCK_VACIO, inc, {self.SKU: 1, "2000": 1}) if h.get("sku") == self.SKU]
        self.assertEqual([h["tipo"] for h in hs], ["dif_wms"])                # antes: ni siquiera salía (Check no tenía el SKU)
        h = hs[0]
        self.assertTrue(h["falta_ingreso_check"])
        self.assertEqual([c["clase"] for c in h["unidades"]["tarjetas"]], ["incidencia", "check_pendiente"])

    def test_un_sku_que_el_erp_no_tiene_en_la_13_no_genera_ruido(self):
        inc = [{"id": 6, "sku": "7777", "descripcion": "Otro", "cantidad": 1, "motivo": "Motivo largo suficiente", "estado": "abierta", "recomendacion": "UA7"}]
        hs = self._todos(self.STOCK_VACIO, inc, {"2000": 1})                  # el ERP no tiene el SKU 7777 en la bodega 13
        self.assertEqual([h for h in hs if h.get("sku") == "7777" and h["tipo"] == "dif_wms"], [])

    def test_si_check_la_tiene_no_se_marca_ingreso_en_check(self):
        wms = [_wms("UA1", "A-1", sku=self.SKU)]
        h = next(x for x in self._todos(wms, [], {self.SKU: 1}) if x.get("sku") == self.SKU)
        self.assertFalse(h["falta_ingreso_check"])
        self.assertEqual([c["clase"] for c in h["unidades"]["tarjetas"]], ["libre"])

    def test_falta_check_con_varias_unidades_se_cuenta_por_diferencia(self):
        wms = [_wms("UA1", "A-1", sku=self.SKU)]                               # el ERP tiene 3, Check 1: faltan 2 en Check
        h = next(x for x in self._todos(wms, [], {self.SKU: 3}) if x.get("sku") == self.SKU)
        self.assertEqual(h["unidades"]["sin_check"], 2)
        self.assertEqual([(c["clase"], c["cantidad"]) for c in h["unidades"]["tarjetas"]], [("libre", 1), ("falta_check", 2)])

    def test_la_pantalla_ofrece_ingreso_en_check_registrar_sin_ua_y_copiar_el_pedido(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        for ancla in ("'Ingresar en Check'", "if(incConcFiltro === 'ingreso') return !!h.falta_ingreso_check;", "Falta ingreso en Check",
                      "var incAltaSinUaOk = false;", "return incAltaSinUaOk || v.trim().length > 0;", "!ua.value.trim() && !incAltaSinUaOk",
                      "incAltaSinUaOk = !!h.falta_ingreso_check && !(h.ua || '').trim();", "function incCopiarPedidoCheck", "data-dif=\"copiar\"",
                      "Registrar ' + (c.cantidad > 1 ? c.cantidad + ' ' : '') + 'sin UA aún"):
            self.assertIn(ancla, t, ancla)
        # una incidencia que aún no tiene UA se puede seguir gestionando
        self.assertIn("incAltaSinUaOk = !((r.recomendacion || '').trim());", t)
        # «Gestionar» sale para toda diferencia con detalle por unidad, también la que aún no tiene incidencia
        self.assertIn("      if(h.unidades){", t)

    def test_el_pedido_a_check_no_escribe_en_check(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        f = t.split("function incCopiarPedidoCheck")[1][:1400]
        self.assertIn("navigator.clipboard", f)                                 # solo copia un texto: Check es SOLO LECTURA (REGLA #4.4)
        self.assertNotIn("fetch(", f)


class TestDiferenciasDeUa(unittest.TestCase):
    """Daniel (29-oct): «también tiene que identificar diferencias de la UA, si corresponde que se gestione una UA en el WMS». Una incidencia que se declaró
    SIN UA (porque Check aún no ingresaba la unidad) y a la que Check ya le tiene una UA libre: aunque los números cuadren, hay que asignarla."""
    SKU = "1049101054"
    INC_SIN_UA = [{"id": 11, "sku": "1049101054", "descripcion": "ILUS Rower Pro", "cantidad": 1, "motivo": "Falta el asiento completo",
                   "estado": "abierta", "recomendacion": ""}]

    def _todos(self, wms, incs, erp):
        return TestFaltaIngresoEnCheck()._todos(wms, incs, erp)

    def test_los_numeros_cuadran_pero_la_incidencia_no_tiene_ua_y_check_si(self):
        wms = [_wms("UA7001", "ARMADO01", sku=self.SKU, desc="ILUS Rower Pro")]
        hs = [h for h in self._todos(wms, self.INC_SIN_UA, {self.SKU: 1}) if h.get("sku") == self.SKU]
        self.assertEqual([h["tipo"] for h in hs if h["tipo"] in ("dif_erp", "dif_wms")], [])      # no hay diferencia numérica...
        h = next(x for x in hs if x["tipo"] == "asignar_ua")                                         # ...pero sí una diferencia de UA
        self.assertEqual((h["ua"], h["ubicacion"]), ("UA7001", "ARMADO01"))
        self.assertTrue(h["falta_asignar_ua"])
        c = next(x for x in h["unidades"]["tarjetas"] if x["clase"] == "incidencia")
        self.assertEqual((c["ua_sugerida"], c["ubicacion_sugerida"]), ("UA7001", "ARMADO01"))
        self.assertEqual(h["unidades"]["por_asignar"], 1)

    def test_si_la_incidencia_ya_tiene_ua_no_se_sugiere_nada(self):
        inc = [dict(self.INC_SIN_UA[0], recomendacion="UA7001")]
        wms = [_wms("UA7001", "ARMADO01", sku=self.SKU, desc="ILUS Rower Pro")]
        hs = [h for h in self._todos(wms, inc, {self.SKU: 1}) if h.get("sku") == self.SKU]
        self.assertEqual([h for h in hs if h["tipo"] == "asignar_ua"], [])

    def test_la_ua_de_otra_incidencia_no_se_ofrece(self):
        # Check solo tiene UA7001 y ya es de la incidencia 12: la 11 (sin UA) no tiene qué asignarse
        inc = self.INC_SIN_UA + [dict(self.INC_SIN_UA[0], id=12, recomendacion="UA7001")]
        wms = [_wms("UA7001", "ARMADO01", sku=self.SKU, desc="ILUS Rower Pro")]
        hs = [h for h in self._todos(wms, inc, {self.SKU: 2}) if h.get("sku") == self.SKU]
        self.assertEqual([h for h in hs if h["tipo"] == "asignar_ua"], [])

    def test_el_resumen_de_la_api_cuenta_las_diferencias_de_ua(self):
        # el resumen lo arma la ruta de la API (no _inc_calcular_hallazgos): cuenta lo que marca `falta_asignar_ua`
        self.assertIn('"asignar_ua":     sum(1 for h in hallazgos if h.get("falta_asignar_ua"))', texto())

    def test_el_catalogo_de_tipos_conoce_asignar_ua(self):
        src = texto()
        self.assertIn('"asignar_ua": {', src)
        self.assertIn('"label": "Asignar UA de Check"', src)

    def test_la_pantalla_ofrece_asignar_la_ua_y_consulta_sola_si_no_encuentra(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        for ancla in ("asignar_ua:     {ico:'bi-upc-scan'", "'Asignar UA'", "if(incConcFiltro === 'asignar') return !!h.falta_asignar_ua;",
                      "data-dif=\"asignar\"", "c.ua_sugerida", "function incAbrirPorId(id, abrirRepuesto, alAbrir)",
                      "En el ERP · falta la gestión en Check", "incPorQueTimer = setTimeout("):
            self.assertIn(ancla, t, ancla)
        # asignar la UA solo rellena el campo: la persona revisa y guarda (nada se escribe sin su confirmación)
        f = t.split("quehacer === 'asignar'")[1][:700]
        self.assertNotIn("fetch(", f)


class TestBajaInteligenteConPosiciones(unittest.TestCase):
    """Daniel (28-oct): «es importante llevar una trazabilidad y poder dar de baja… un proceso inteligente, corto, intuitivo y con las bodegas, ya que puedes
    identificar las posiciones en Check. Solo yo puedo eliminar productos de la base de datos»."""
    STOCK = [{"ua": "UA1", "bodega": "BODEGA 13", "ubicacion": "ARMADO01", "stFisico": 1, "codigo": "S1"},
             {"ua": "UA2", "bodega": "BODEGA 05", "ubicacion": "A-12", "stFisico": 1, "codigo": "S1"}]

    def _f(self):
        return cargar(("_inc_posiciones_check", "_inc_baja_sugerencia"), ("INC_BODEGA_WMS",))

    def test_posiciones_de_una_ua(self):
        p = self._f()["_inc_posiciones_check"](self.STOCK, "ua1", "S1")
        self.assertEqual(p, [{"ua": "UA1", "bodega": "BODEGA 13", "ubicacion": "ARMADO01", "cant": 1.0}])

    def test_sin_ua_se_busca_por_sku_con_la_bodega_13_primero(self):
        p = self._f()["_inc_posiciones_check"](list(reversed(self.STOCK)), None, "S1")
        self.assertEqual([x["bodega"] for x in p], ["BODEGA 13", "BODEGA 05"])

    def test_si_salio_de_la_bodega_13_sugiere_salio_resuelto_y_dice_donde_esta(self):
        f = self._f()
        pos = f["_inc_posiciones_check"](self.STOCK, "UA2", "S1")
        s = f["_inc_baja_sugerencia"](pos, "UA2")
        self.assertEqual(s["codigo"], "salio_resuelto")
        self.assertIn("BODEGA 05 · A-12", s["razon"])

    def test_si_ya_no_aparece_en_check(self):
        s = self._f()["_inc_baja_sugerencia"]([], "UA9")
        self.assertEqual((s["codigo"], s["razon"]), ("salio_resuelto", "La UA ya no aparece en Check."))

    def test_si_los_tres_sistemas_cuadran_sugiere_regularizado(self):
        f = self._f()
        pos = f["_inc_posiciones_check"](self.STOCK, "UA1", "S1")
        self.assertEqual(f["_inc_baja_sugerencia"](pos, "UA1", 1, 1, 1)["codigo"], "regularizado_random")
        self.assertIsNone(f["_inc_baja_sugerencia"](pos, "UA1", 1, 2, 2))             # hay diferencia: no se sugiere nada

    def test_la_baja_guarda_la_posicion_con_respaldo_si_faltan_las_columnas(self):
        src = texto("app.py")
        for ancla in ("bodega_check, ubicacion_check", "sin columnas de posición", "_inc_baja_posicion_snapshot(", "def _ensure_incidencia_bajas_posicion"):
            self.assertIn(ancla, src, ancla)
        self.assertIn("_ensure_incidencia_bajas_posicion()", src.split("def _ensure_incidencia_bajas_table")[-1])
        # la posición la calcula el SERVIDOR: el navegador no la manda
        self.assertNotIn("bodega_check", json.dumps([]))

    def test_el_modal_muestra_posiciones_sugiere_y_el_historial_las_guarda(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        for ancla in ('id="incBajaPos"', "incBajaContexto(ctx);", "Dónde está hoy en Check", "Sugerido", "b.bodega_check"):
            self.assertIn(ancla, t, ancla)
        self.assertIn("_incBaja.motivo = d.sugerencia.codigo", t)                       # queda preseleccionado: la persona solo confirma

    def test_eliminar_solo_para_el_superadmin_tambien_en_la_pantalla(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn("var incCanDelete = {{ 'true' if permissions.superadmin else 'false' }};", t)
        self.assertRegex(t, r"\{% if permissions\.superadmin %\}\s*<button type=\"button\" class=\"btn btn-outline-danger me-auto\" id=\"incBtnEliminar\"")


class TestSepararEnFilasOUnaLinea(unittest.TestCase):
    """Daniel: «se puede mantener en una línea a menos que el usuario decida separarla; si no, lo más recomendable es pedir crear varias filas si
    el ingreso es mayor a uno»."""

    def _asignar(self, h, n):
        return cargar(("_inc_asignar_uas_grupos",))["_inc_asignar_uas_grupos"](h, n)

    def test_cada_fila_toma_su_propia_ua_libre_de_check(self):
        h = {"uas_libres": [{"ua": "ua2", "ubicacion": "B-2"}, {"ua": "UA3", "ubicacion": "B-3"}], "ua": "UA2", "baja_hallazgo": False}
        self.assertEqual(self._asignar(h, 2), [("UA2", "B-2"), ("UA3", "B-3")])

    def test_si_hay_menos_ua_libres_que_filas_las_demas_quedan_sin_ua(self):
        h = {"uas_libres": [{"ua": "UA2", "ubicacion": "B-2"}]}
        self.assertEqual(self._asignar(h, 3), [("UA2", "B-2"), (None, None), (None, None)])

    def test_un_pendiente_de_una_sola_ua_conserva_la_ua_en_la_primera_fila_como_antes(self):
        h = {"baja_hallazgo": True, "ua": "ua9", "ubicacion": "C-1"}
        self.assertEqual(self._asignar(h, 2), [("UA9", "C-1"), (None, None)])

    def test_sin_ua_libre_y_con_incidencia_no_se_reutiliza_la_ua_en_uso(self):
        h = {"baja_hallazgo": False, "ua": "UA1014361", "uas_libres": []}
        self.assertEqual(self._asignar(h, 2), [(None, None), (None, None)])

    def test_el_servidor_separa_tambien_un_sku_que_ya_tiene_incidencia(self):
        src = texto("app.py").split("def mant_api_incidencias_desglosar")[1][:9000]
        self.assertIn('not h.get("baja_hallazgo") and not h.get("puede_registrar")', src)
        self.assertIn('int(h.get("faltan") or 0) if h.get("puede_registrar")', src)
        self.assertIn("_inc_asignar_uas_grupos(h, len(limpios), _checkwms_norm_ua)", src)
        self.assertIn("_inc_ua_duplicada(ua_k)", src)                  # una UA en uso nunca se reutiliza

    def test_el_front_pregunta_con_dos_o_mas_unidades_y_recomienda_segun_el_producto(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn('id="incAgruparModal"', t)
        reg = t.split("function incRegistrarConPregunta")[1][:400]
        self.assertIn("if(n < 2 || !h.clave){ incAbrirAltaPrellenada(h); return; }", reg)       # 1 unidad: directo, sin preguntar
        preg = t.split("function incPreguntarAgrupar")[1][:2600]
        self.assertIn("var agrupable = (h.repetible === true)", preg)                          # repetible → recomienda una línea; el resto → separar
        self.assertIn("porUnidad:true", preg)
        self.assertIn("incAbrirAltaPrellenada(h)", preg)                                       # «una línea» sigue siendo el alta de siempre
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", preg))

    def test_todos_los_caminos_de_registrar_pasan_por_la_pregunta(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn("if(h.accion === 'registrar'){ incRegistrarConPregunta(h); return; }", t)
        self.assertIn("else incRegistrarConPregunta(Object.assign({}, h,", t)
        # Solo 3 puntos de entrada al alta: su definición, «1 sola unidad» (directo) y «dejar en una línea» (después de la pregunta). Ningún camino la salta.
        self.assertEqual(t.count("incAbrirAltaPrellenada("), 3)


class TestElBuscador(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(PLANTILLA, encoding="utf-8") as fh:
            cls.t = fh.read()

    def test_la_barra_existe_y_vive_fuera_de_la_tabla_que_se_repinta(self):
        self.assertIn('id="incConcQ"', self.t)
        self.assertLess(self.t.index('id="incConcBuscar"'), self.t.index('id="incConcLista"'))
        self.assertIn("incConcCoincide(h, qn)", self.t)
        self.assertIn("$('incConcBuscar').hidden = (incConcFiltro === 'bajas')", self.t)

    def test_la_logica_busca_sin_tildes_en_ua_sku_producto_y_ubicacion(self):
        exe = shutil.which("node")
        if not exe:
            self.skipTest("Node no está instalado")
        norm = re.search(r"function incConcNorm\(s\)\{.*?\n  \}", self.t, re.S).group(0)
        coin = re.search(r"function incConcCoincide\(h, q\)\{.*?\n  \}", self.t, re.S).group(0)
        prog = (norm + "\n" + coin + "\n" +
                "const h={ua:'UA1014362',sku:'1050100999',descripcion:'ILUS Air Ski Erg Machine',ubicacion:'B-02-3',detalle:'ERP Random dice 2'};"
                "process.stdout.write(JSON.stringify([incConcCoincide(h,''),incConcCoincide(h,incConcNorm('ua1014362')),incConcCoincide(h,incConcNorm('ski ERG')),"
                "incConcCoincide(h,incConcNorm('1050100999')),incConcCoincide(h,incConcNorm('b-02')),incConcCoincide(h,incConcNorm('bicicleta')),"
                "incConcCoincide({descripcion:'Máquina de remo'},incConcNorm('maquina'))]));")
        r = subprocess.run([exe, "-e", prog], capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), [True, True, True, True, True, False, True])

    def test_el_script_de_la_pagina_sigue_compilando(self):
        exe = shutil.which("node")
        if not exe:
            self.skipTest("Node no está instalado")
        bloques = re.findall(r"<script>(.*?)</script>", self.t, re.S)
        mayor = max(bloques, key=len)
        mayor = re.sub(r"\{#.*?#\}", "", mayor, flags=re.S)          # el JS de la página lleva trozos de Jinja: se neutralizan para compilarlo
        mayor = re.sub(r"\{%.*?%\}", "", mayor, flags=re.S)
        mayor = re.sub(r"\{\{.*?\}\}", "0", mayor, flags=re.S)
        r = subprocess.run([exe, "--check", "-"], input=mayor, capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr)


class TestAccionesYModal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(PLANTILLA, encoding="utf-8") as fh:
            cls.t = fh.read()

    def test_registrar_aparece_junto_a_ver_corregir_cuando_faltan_unidades(self):
        self.assertIn("if(h.puede_registrar){", self.t)
        self.assertIn('data-accion="registrar"', self.t)
        self.assertIn("if(subaccion === 'registrar' && h.puede_registrar){ incRegistrarFaltante(h); return; }", self.t)

    def test_sin_ua_libre_se_explica_en_vez_de_abrir_un_alta_que_el_servidor_rechazaria(self):
        self.assertIn("No hay otra UA libre en Check", self.t)
        self.assertIn("ilusAlert(", self.t.split("function incRegistrarFaltante")[1][:700])

    def test_el_modal_es_ancho_con_las_tarjetas_1_a_4_en_dos_columnas(self):
        self.assertIn('modal-dialog modal-xl inc-modal-ancho modal-dialog-scrollable', self.t)
        css = self.t.split("@media (min-width:1100px){")[1][:900]
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", css)
        for n in "1234":
            self.assertIn('.inc-pane[data-pane="%s"]' % n, css)
        self.assertNotIn('data-pane="5"]', css)                      # Gestión de repuestos y Evidencia siguen a ancho completo

    def test_el_alta_deja_elegir_entre_varias_ua_libres(self):
        self.assertIn('id="incUaAlternas"', self.t)
        self.assertIn("incPintarUasAlternas(h);", self.t)
        self.assertIn("incPintarUasAlternas(null);", self.t)            # limpiarModal no deja las UA de la vez anterior

    def test_nada_de_alert_confirm_prompt_nativos_en_lo_nuevo(self):
        nuevo = self.t.split("function incRegistrarFaltante")[1][:2600]
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", nuevo))


class TestFuentes(unittest.TestCase):
    def test_el_backend_calcula_faltan_en_el_mismo_lugar_que_la_accion(self):
        src = texto("app.py")
        self.assertIn('h["faltan"] = _inc_hallazgo_faltan(h)', src)
        self.assertIn('h["puede_registrar"] = h["faltan"] > 0', src)


if __name__ == "__main__":
    unittest.main()

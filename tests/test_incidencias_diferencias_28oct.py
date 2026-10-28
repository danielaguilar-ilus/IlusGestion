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

from tests._fuente_app import cargar, texto

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANTILLA = os.path.join(RAIZ, "templates", "mantenciones", "incidencias.html")


class _IncConcMysqlError(Exception):
    pass


def _wms(ua, ubicacion, sku="1050100999", desc="ILUS Air Ski Erg Machine", cant=1):
    return {"bodega": "BODEGA 13", "ua": ua, "codigo": sku, "descripcion": desc, "stFisico": cant, "ubicacion": ubicacion}


def _hallazgos(wms, incs, erp):
    amb = cargar(
        ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_calcular_hallazgos"),
        ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
        extra={
            "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda: wms,
            "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda: erp,
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
            ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_calcular_hallazgos"),
            ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
            extra={
                "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda: wms,
                "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda: erp,
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
        panel = t.split("function incAbrirDiferencia")[1][:6500]
        for ancla in ("incAbrirPorId(c.id)", "incAbrirBaja(", "incRegistrarConPregunta(Object.assign", "con motivo", "Falta agregar", "Registrar esta UA"):
            self.assertIn(ancla, panel, ancla)
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", panel))
        # «Ver / corregir» no se pierde: queda en el menú de la fila (REGLA #4.2)
        self.assertIn("Ver / corregir la incidencia", t)


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

# -*- coding: utf-8 -*-
"""Bodega de incidencias → «Gestionar diferencia» → «Ver / corregir» (Daniel, 7-nov-2026).

Caso real: Tarro Pegamento U Lock Turf 12,5 kg (SKU TPT-12). Nosotros 14, ERP 7, Check 7: dos incidencias de 7 (UA1006507 y UA1011973), las dos
con motivo. Al tocar «Ver / corregir» no pasaba nada. Causa: el botón pedía la lista con page_size=500 (el servidor solo acepta hasta 200 → devolvía
10 filas) y SIN ver_todo, que deja fuera justo las incidencias que no cuadran. La incidencia nunca llegaba y no se abría nada, sin aviso.

Además el panel decía «declaramos 7 de más» sin decir cuál sobra, con la barra en verde, y llamaba «equipo» a un tarro sin clasificar.
Las funciones de app.py corren sobre una base y un WMS de mentira (tests/_fuente_app.py); el JS de la página se ejecuta con Node."""
import json
import os
import re
import shutil
import subprocess
import unittest

from tests._fuente_app import cargar, fuente_funcion

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANTILLA = os.path.join(RAIZ, "templates", "mantenciones", "incidencias.html")


class _IncConcMysqlError(Exception):
    pass


def _wms(ua, cant, sku="TPT-12", desc="Tarro Pegamento U Lock Turf 12,5 kg", ubicacion="P-01"):
    return {"bodega": "BODEGA 13", "ua": ua, "codigo": sku, "descripcion": desc, "stFisico": cant, "ubicacion": ubicacion}


INCS = [
    {"id": 55, "sku": "TPT-12", "descripcion": "Tarro Pegamento U Lock Turf 12,5 kg", "cantidad": 7,
     "motivo": "tarros en bodega incidencia almacenaje", "estado": "abierta", "recomendacion": "UA1006507"},
    {"id": 154, "sku": "TPT-12", "descripcion": "Tarro Pegamento U Lock Turf 12,5 kg", "cantidad": 7,
     "motivo": "Tarros de pegamento bajo la custodia de SSTT.", "estado": "abierta", "recomendacion": "UA1011973"},
]


def _hallazgo(wms, incs, erp, clasif=None, tipo="dif_erp"):
    amb = cargar(
        ("_inc_uas_libres", "_inc_hallazgo_faltan", "_inc_hallazgo_accion", "_inc_unidades_hallazgo", "_inc_consolidar_por_sku",
         "_inc_pendiente_dividir", "_inc_calcular_hallazgos"),
        ("INC_BODEGA_WMS", "INC_BODEGA_ERP"),
        extra={
            "_IncConcMysqlError": _IncConcMysqlError, "_checkwms_stock_rows": lambda forzar=False: wms,
            "mysql_fetchall": lambda sql, params=None: incs, "_erp_stock_b13_por_sku": lambda forzar=False: erp,
            "_checkwms_norm_ua": lambda s: (s or "").strip().upper(),
            "_inc_clasificacion_skus_batch": lambda skus: dict(clasif or {}),
            "_inc_hallazgo_falta_registrar": lambda ua, r, e, c: {"tipo": "falta_registrar", "ua": ua, "sku": r.get("codigo")},
            "_inc_baja_clave": lambda h: "k", "_inc_completar_nombres": lambda hs: None,
        })
    hs = amb["_inc_calcular_hallazgos"]()["hallazgos"]
    return next(x for x in hs if x["tipo"] == tipo)


class TestElServidorEntregaLaIncidenciaQueNoCuadra(unittest.TestCase):
    """«Ver / corregir» pide UNA incidencia por su número, aunque no cuadre."""

    @classmethod
    def setUpClass(cls):
        cls.src = fuente_funcion("mant_api_incidencias_list")

    def test_acepta_id_y_filtra_por_esa_incidencia(self):
        self.assertIn('solo_id = int(request.args.get("id") or 0)', self.src)
        self.assertIn('where.append("id = %s")', self.src)
        self.assertIn("params.append(solo_id)", self.src)

    def test_con_id_no_se_excluyen_las_que_no_cuadran(self):
        self.assertIn('if (request.args.get("ver_todo") or "").strip() != "1" and not solo_id:', self.src)

    def test_con_id_siguen_fuera_las_eliminadas_y_las_dadas_de_baja(self):
        ini = self.src.index('where = ["COALESCE(eliminada,0) = 0"]')
        self.assertLess(ini, self.src.index("solo_id = int("))
        self.assertIn("ids_baja = _inc_ids_en_baja()", self.src)       # la baja no depende de solo_id


class TestElPanelDiceCualSobra(unittest.TestCase):
    def test_tarro_de_pegamento_check_tiene_una_ua_y_la_otra_no(self):
        h = _hallazgo([_wms("UA1006507", 7)], INCS, {"TPT-12": 7})
        u = h["unidades"]
        self.assertEqual((u["unidades"], u["con_motivo"], u["faltan"], u["sobran"]), (14, 14, 0, 7))
        t = {c["id"]: c for c in u["tarjetas"] if c["clase"] == "incidencia"}
        self.assertEqual((t[55]["en_check"], t[55]["check_cant"]), (True, 7))       # coincide
        self.assertEqual((t[154]["en_check"], t[154]["check_cant"]), (False, 0))    # la que puede sobrar

    def test_una_ua_que_check_reparte_en_varias_filas_se_suma(self):
        h = _hallazgo([_wms("UA1006507", 3, ubicacion="P-01"), _wms("UA1006507", 4, ubicacion="P-02")], INCS[:1], {"TPT-12": 8})
        c = next(c for c in h["unidades"]["tarjetas"] if c["clase"] == "incidencia")
        self.assertEqual(c["check_cant"], 7)

    def test_si_check_no_respondio_no_se_afirma_nada(self):
        # Check caído: sin filas. El ERP sí respondió y hay diferencia: la tarjeta no dice «no está en Check».
        h = _hallazgo([], INCS, {"TPT-12": 7})
        for c in h["unidades"]["tarjetas"]:
            if c["clase"] == "incidencia":
                self.assertNotIn("en_check", c)
                self.assertNotIn("check_cant", c)

    def test_sin_dato_de_check_la_funcion_pura_sigue_como_antes(self):
        amb = cargar(("_inc_unidades_hallazgo",))
        u = amb["_inc_unidades_hallazgo"]({"tipo": "dif_erp", "erp": 7, "wms": 7, "nuestra_bd": 14},
                                          [{"id": 55, "ua": "UA1006507", "motivo": "x" * 12, "cantidad": 7}], False)
        self.assertNotIn("en_check", u["tarjetas"][0])

    def test_un_producto_sin_clasificar_no_se_llama_equipo(self):
        h = _hallazgo([_wms("UA1006507", 7)], INCS, {"TPT-12": 7}, clasif={})
        self.assertTrue(h["sin_clasificar"])
        self.assertFalse(h["repetible"])
        h2 = _hallazgo([_wms("UA1006507", 7)], INCS, {"TPT-12": 7}, clasif={"TPT-12": {"repetible": False}})
        self.assertFalse(h2["sin_clasificar"])
        h3 = _hallazgo([_wms("UA1006507", 7)], INCS, {"TPT-12": 7}, clasif={"TPT-12": {"repetible": True}})
        self.assertTrue(h3["repetible"])
        self.assertFalse(h3["sin_clasificar"])


class TestLaPagina(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(PLANTILLA, encoding="utf-8") as fh:
            cls.t = fh.read()
        cls.panel = cls.t.split("function incAbrirDiferencia")[1].split("function incPintarUasAlternas")[0]

    def test_ya_no_pide_500_filas_sin_ver_todo(self):
        src = self.t.split("function incAbrirPorId")[1][:2200]
        self.assertIsNone(re.search(r"fetch\([^)]*page_size=500", self.t), "queda un fetch con page_size=500")
        self.assertIn("?id=' + encodeURIComponent(id) + '&ver_todo=1", src)

    def test_si_no_llega_se_avisa_con_el_modal_de_ilus(self):
        src = self.t.split("function incAbrirPorId")[1][:2200]
        self.assertIn("ilusAlert(", src)
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", src))

    def test_el_panel_dice_que_hacer_cuando_declaramos_de_mas(self):
        for ancla in ("de más</b>", "Revisar: puede sobrar", "no tiene</b> esta UA en la bodega 13", "inc-dif-prog' + (u.sobran ? ' alerta'",
                      "no está clasificado</b> en el Catálogo"):
            self.assertIn(ancla, self.panel, ancla)
        self.assertIsNone(re.search(r"(?<![A-Za-z.])(alert|confirm|prompt)\(", self.panel))

    def test_lo_que_se_abre_desde_el_panel_vuelve_al_panel(self):
        self.assertIn("_incDifVolver = {sku: h.sku, tipo: h.tipo};", self.panel)
        self.assertIn("{cerrar: $('incDifModal')}", self.panel)
        self.assertIn("incConcCargar(false, function(d){", self.t)
        self.assertIn("if(typeof alListo === 'function') alListo(d);", self.t)
        for m in ("'incModal'", "'incBajaModal'", "'incAgruparModal'", "'incDesgloseModal'"):
            self.assertIn(m, self.t.split("var INC_DIF_FLUJO")[1][:200])

    def _js(self, prog):
        exe = shutil.which("node")
        if not exe:
            self.skipTest("Node no está instalado")
        a = re.search(r"  function incAbrirPorId\(id, abrirRepuesto, alAbrir, opts\)\{.*?\n  \}\n", self.t, re.S).group(0)
        b = re.search(r"  function incCerrarYLuego\(el, fn\)\{.*?\n  \}\n", self.t, re.S).group(0)
        r = subprocess.run([exe, "-e", a + b + prog], capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr)
        return json.loads(r.stdout)

    def test_js_abre_la_incidencia_que_llega_por_id(self):
        out = self._js(
            "var pedidas=[], abiertas=[], avisos=[];"
            "global.fetch=function(u){pedidas.push(u);return Promise.resolve({json:function(){return {ok:true,rows:[{id:154,sku:'TPT-12'}]};}});};"
            "global.abrirEdicion=function(id,rows){abiertas.push([id,rows.length]);};"
            "global.ilusAlert=function(o){avisos.push(o.title);}; global.ilusToast=function(){};"
            "incAbrirPorId(154).then(function(ok){process.stdout.write(JSON.stringify({ok:ok,pedidas:pedidas,abiertas:abiertas,avisos:avisos}));});")
        self.assertTrue(out["ok"])
        self.assertEqual(out["abiertas"], [[154, 1]])
        self.assertEqual(out["avisos"], [])
        self.assertIn("id=154", out["pedidas"][0])
        self.assertIn("ver_todo=1", out["pedidas"][0])

    def test_js_si_ya_no_existe_avisa_en_vez_de_quedarse_callado(self):
        out = self._js(
            "var abiertas=[], avisos=[];"
            "global.fetch=function(u){return Promise.resolve({json:function(){return {ok:true,rows:[]};}});};"
            "global.abrirEdicion=function(id,rows){abiertas.push(id);};"
            "global.ilusAlert=function(o){avisos.push(o.title);}; global.ilusToast=function(){};"
            "incAbrirPorId(154).then(function(ok){process.stdout.write(JSON.stringify({ok:ok,abiertas:abiertas,avisos:avisos}));});")
        self.assertFalse(out["ok"])
        self.assertEqual(out["abiertas"], [])
        self.assertEqual(out["avisos"], ["No se pudo abrir la incidencia #154"])

    def test_js_cierra_el_panel_antes_de_abrir_la_incidencia(self):
        out = self._js(
            "var orden=[];"
            "var panel={classList:{contains:function(c){return c==='show';}},_h:null,"
            "  addEventListener:function(ev,fn){this._h=fn;}};"
            "global.bootstrap={Modal:{getOrCreateInstance:function(el){return {hide:function(){orden.push('panel cerrado');setTimeout(el._h,0);}};}}};"
            "global.fetch=function(u){return Promise.resolve({json:function(){return {ok:true,rows:[{id:55}]};}});};"
            "global.abrirEdicion=function(id){orden.push('abre '+id);};"
            "global.ilusAlert=function(){}; global.ilusToast=function(){};"
            "incAbrirPorId(55,false,null,{cerrar:panel}).then(function(){setTimeout(function(){process.stdout.write(JSON.stringify(orden));},10);});")
        self.assertEqual(out, ["panel cerrado", "abre 55"])

    def test_el_script_de_la_pagina_sigue_compilando(self):
        exe = shutil.which("node")
        if not exe:
            self.skipTest("Node no está instalado")
        bloques = re.findall(r"<script>(.*?)</script>", self.t, re.S)
        mayor = max(bloques, key=len)
        mayor = re.sub(r"\{#.*?#\}", "", mayor, flags=re.S)
        mayor = re.sub(r"\{%.*?%\}", "", mayor, flags=re.S)
        mayor = re.sub(r"\{\{.*?\}\}", "0", mayor, flags=re.S)
        r = subprocess.run([exe, "--check", "-"], input=mayor, capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()

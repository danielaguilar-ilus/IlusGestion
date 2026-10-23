# -*- coding: utf-8 -*-
"""2026-10-23 · OT-2026-00348 de VF Sports (Isabel en terreno). Daniel:

  1. «Se están filtrando productos del servicio: descuentos, ZZENVIO, ZZINSTALACION, ya que son servicios»: «DESCUENTO VENTAS» (SKU «DE»,
     cuya «cantidad» es el MONTO) llegó a la OT como un equipo más, con su checklist. Una sola definición (`_sku_es_servicio`) y todos los
     caminos que arman equipos la usan; lo que ya nació así deja de bloquear la firma y de dibujarse.
  2. «En el formulario de accesorios… solo colocar si llegó o no y el formulario chico de foto y descripción opcional, nada más, ya que los
     accesorios no se instalan»: un ACCESORIO (clase «accesorio» del Catálogo) contesta UNA pregunta («¿Llegó?»), sin checklist.

Se prueban las funciones puras con una base de mentira (nada toca producción) y se vigila por texto que cada camino use la regla.
"""
import os
import re
import unittest

from tests.test_privacidad_proveedores_tecnicos import _extraer

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


class TestSkuEsServicio(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.amb = _extraer(["_sku_es_servicio"], extra={"re": re}, constantes=("_SERVICIO_NOMBRE_RE",))

    def test_servicios_y_descuento(self):
        f = self.amb["_sku_es_servicio"]
        for sku in ("ZZENVIO", "zzinstalacion", " ZZMANTENCION ", "DE", " de "):
            self.assertTrue(f(sku), sku)
        self.assertTrue(f("", "DESCUENTO VENTAS"))
        self.assertTrue(f(None, "  Descuento especial"))

    def test_productos_reales_no_son_servicio(self):
        f = self.amb["_sku_es_servicio"]
        for sku, nombre in (("78184937", "ILUS Core Roller Platinum"), ("DEF001", "Disco"), ("ABC", "Banda de resistencia"),
                            ("A1", "Con descuento incluido")):
            self.assertFalse(f(sku, nombre), sku)


class _BD:
    """Base de mentira: contesta por un trozo del SQL."""
    def __init__(self, tareas_maquinas, clases, respondidos_ve=(), respondidos_pe=(), falla=False):
        self.tm, self.clases, self.ve, self.pe, self.falla = tareas_maquinas, clases, set(respondidos_ve), set(respondidos_pe), falla

    def fetchall(self, sql, params=()):
        if self.falla:
            raise RuntimeError("base caída")
        if "SELECT DISTINCT m.id, m.sku" in sql:
            return [{"id": i, "sku": s} for i, s in self.tm]
        if "estado_revision IN ('saltado','falla_detectada')" in sql:
            return [{"maquina_id": i} for i in self.ve]
        if "FROM mant_ot_producto_estado" in sql:
            return [{"maquina_id": i} for i in self.pe]
        if "SELECT id, nombre FROM mant_maquinas" in sql:
            return [{"id": i, "nombre": f"Accesorio {i}"} for i, _s in self.tm]
        return []


class TestAccesoriosDeLaOT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.G = {}
        cls.amb = _extraer(["_ot_accesorios_ids", "_ot_accesorios_estado"], extra=cls.G)

    def _preparar(self, bd, otpe_ok=True):
        # las funciones extraídas usan `self.amb` como sus globales: se inyecta ahí (no en la copia `extra`)
        self.amb["mysql_fetchall"] = bd.fetchall
        self.amb["_inc_clasificacion_skus_batch"] = lambda skus: {s: {"clase": bd.clases.get(s)} for s in skus if s in bd.clases}
        self.amb["_otpe_ok"] = lambda: otpe_ok

    def test_solo_los_de_clase_accesorio(self):
        bd = _BD([(1, "BAND-1"), (2, "CINTA-9"), (3, "SIN-CLASE")], {"BAND-1": "accesorio", "CINTA-9": "trotadora"})
        self._preparar(bd)
        self.assertEqual(self.amb["_ot_accesorios_ids"](7), {1})

    def test_sin_respuesta_hasta_que_contesta(self):
        bd = _BD([(1, "BAND-1"), (2, "BAND-2")], {"BAND-1": "accesorio", "BAND-2": "accesorio"})
        self._preparar(bd)
        est = self.amb["_ot_accesorios_estado"](7)
        self.assertEqual((est["total"], est["ok"]), (2, 0))
        self.assertEqual([a["id"] for a in est["sin_respuesta"]], [1, 2])
        bd.pe = {1}                       # «Sí, llegó» del 1
        est = self.amb["_ot_accesorios_estado"](7)
        self.assertEqual((est["total"], est["ok"]), (2, 1))
        self.assertEqual([a["id"] for a in est["sin_respuesta"]], [2])
        bd.ve = {2}                       # el 2 quedó «saltado» (No llegó): también cuenta como contestado
        est = self.amb["_ot_accesorios_estado"](7)
        self.assertEqual((est["total"], est["ok"], est["sin_respuesta"]), (2, 2, []))

    def test_fail_open_si_la_base_falla(self):
        bd = _BD([(1, "BAND-1")], {"BAND-1": "accesorio"}, falla=True)
        self._preparar(bd)
        self.assertEqual(self.amb["_ot_accesorios_ids"](7), set())
        est = self.amb["_ot_accesorios_estado"](7)
        self.assertEqual((est["total"], est["ok"], est["sin_respuesta"]), (0, 0, []),
                         "sin dato no se inventa una exigencia que trabe la firma")


class TestRegistroLlego(unittest.TestCase):
    def test_nada_es_obligatorio(self):
        import ot_producto_estado as pe
        r = pe.normalizar_llego({}, n_fotos=0)
        self.assertEqual((r["estado"], r["detalle"], r["fotos_validas"]), ("llego", None, 0))
        r = pe.normalizar_llego({"descripcion": "  Llegó  con   bolsa  "}, n_fotos=2)
        self.assertEqual((r["detalle"], r["fotos_validas"]), ("Llegó con bolsa", 2))
        self.assertIn("Llegó — Llegó con bolsa", r["observacion"])
        self.assertLessEqual(len(pe.normalizar_llego({"descripcion": "x" * 5000})["detalle"]), pe.MAX_DESCRIPCION_LLEGO)

    def test_chip_y_boton(self):
        import ot_producto_estado as pe
        self.assertEqual(pe.chip_equipo({"estado": "llego", "creado_por": "Isabel Rojas"}, "23/10"), "Llegó · 23/10 · Isabel")
        self.assertEqual(pe.BOTON_EQUIPO["llego"], "Sí, llegó")
        # «No llegó» sigue siendo una de las tres salidas de siempre (no se tocó su validación)
        self.assertNotIn("llego", pe.ESTADOS_EQUIPO)
        self.assertIsNone(pe.normalizar_equipo("llego", {"motivo_clave": "x"})[0])


class TestCaminosUsanLaRegla(unittest.TestCase):
    """Texto: cada camino que arma equipos o cuenta obligatorias usa la regla única."""

    @classmethod
    def setUpClass(cls):
        cls.app = _leer("app.py")

    def _funcion(self, nombre, largo=9000):
        i = self.app.index(f"def {nombre}(")
        return self.app[i:i + largo]

    def test_excluidos_del_cierre_incluyen_servicios_y_accesorios(self):
        f = self._funcion("_ot_maquinas_excluidas_cierre", 4200)
        self.assertIn("LIKE 'ZZ%%'", f)
        self.assertIn("_ot_accesorios_ids(vid)", f)

    def test_la_respuesta_del_accesorio_cuenta_como_una_obligatoria(self):
        f = self._funcion("_ot_chk_revision_pendientes", 9000)
        self.assertIn("_ot_accesorios_estado(vid)", f)
        self.assertIn('"motivo": "acc_llego"', f)
        self.assertIn('out["kpi_delta"]["obl"] += acc["total"]', f)
        self.assertIn("falta indicar si llegó", f)

    def test_endpoint_llego_solo_accesorios_y_con_los_mismos_candados(self):
        i = self.app.index('@app.route("/ot/api/<int:vid>/equipo/<int:mid>/llego"')
        f = self.app[i:i + 7000]
        self.assertIn("@_ot_can_configurar", f.split("def ot2_api_equipo_llego")[0])
        for pedazo in ("NO_ES_ACCESORIO", "_ot_accesorios_ids(vid)", "'verificado'", "OT_CERRADA", "EQUIPO_YA_MARCADO",
                       "normalizar_llego", "_OTPE_MAX_FOTOS"):
            self.assertIn(pedazo, f, pedazo)
        # foto y descripción son OPCIONALES: nada en la ruta las exige
        self.assertNotIn("FOTO_REQUERIDA", f)
        self.assertNotIn("DESCRIPCION_REQUERIDA", f)

    def test_servicios_no_se_dan_de_alta_ni_se_ofrecen_como_equipo(self):
        for nombre in ("mant_maquina_add", "_ot2_crear_core", "mant_cliente_maquinas_list", "_ot_equipos_desde_doc_core",
                       "_ficha_equipos_desde_instalacion"):
            self.assertIn("_sku_es_servicio(", self._funcion(nombre, 120000), nombre)
        i = self.app.index("def ot2_detalle(")
        self.assertIn("NOT LIKE 'ZZ%%'", self.app[i:i + 30000], "el listado de equipos de la ficha de la OT no dibuja servicios")

    def test_pantallas_filtran_el_descuento(self):
        tka = _leer("templates", "tickets", "_tka_modal.html")
        self.assertIn("function _tkaEsServicio(", tka)
        self.assertEqual(tka.count(".filter(l => !l.es_zz)"), 0, "el filtro viejo (solo ZZ) ya no queda")
        crear = _leer("templates", "ot2", "_modal_crear.html")
        self.assertIn("function _o2mEsServicio(", crear)
        self.assertNotIn("return !l.es_zz && l.sku;", crear)


class TestPantallaDelAccesorio(unittest.TestCase):
    def test_barra_y_flujo(self):
        js = _leer("static", "ot_acciones.js")
        for pedazo in ("function accHTML(", "if (d.acc && d.k !== 'i') return accHTML(key, d);", "Sí, llegó", "accnollego",
                       "var llego = {", "llego: llego }", "'/llego'"):
            self.assertIn(pedazo, js, pedazo)
        # el formulario chico: foto y descripción opcionales, sin lista de motivos ni textos mínimos
        flujo = js[js.index("var llego = {"):js.index("var FLUJO = {")]
        self.assertEqual(flujo.count("'Opcional'"), 2)
        self.assertNotIn("faltaBtn(", flujo)

    def test_tarjeta_cuenta_una_obligatoria(self):
        html = _leer("templates", "ot2", "detalle.html")
        for pedazo in ("_es_acc = (e.es_accesorio and not e._interno)", 'data-acc="{{ \'1\' if _es_acc else \'\' }}"',
                       "Los accesorios no se instalan", "otd-wk-acc"):
            self.assertIn(pedazo, html, pedazo)
        self.assertIn("(1 if e.acc_respondido else 0) if _es_acc", html)


if __name__ == "__main__":
    unittest.main()

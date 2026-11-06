# -*- coding: utf-8 -*-
"""📑 6-nov-2026 — «Detalle de cálculo» de una cotización rediseñado «para las dudas» (Daniel: «reparar ese PDF, hacerlo con el diseño para las dudas más adelante»):
cada ítem dice DE DÓNDE salió su precio, cuánto cuesta y cuánto margen deja de verdad (antes mostraba siempre «costo + margen» aunque el precio fuera escrito a mano).
Correr:  python -m pytest tests/test_cot_detalle_calculo_dudas.py -q -p no:warnings"""
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import cot_detalle_calculo as cd  # noqa: E402
from jinja2 import Environment, FileSystemLoader  # noqa: E402


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PY = _leer("tickets_module.py")
PLANTILLA = _leer("templates", "tickets", "cotizacion_detalle_calculo.html")


def _a(**kw):
    base = dict(cantidad=1, precio_unitario=65000, total=65000, horas=2.5, tecnicos=1, valor_hh=20000, margen_pct=30)
    base.update(kw)
    return cd.analizar_item(**base)


class TestAnalisisDeUnItem(unittest.TestCase):
    def test_automatico_coincide_con_costo_mas_margen(self):
        a = _a()
        self.assertEqual(a["origen"], "automatico")
        self.assertEqual((a["costo_unit"], a["precio_hoy"], a["margen_unit"], a["margen_pct_real"]), (50000, 65000, 15000, 30.0))
        self.assertFalse(a["difiere_hoy"])

    def test_precio_escrito_a_mano_muestra_el_margen_REAL_no_el_de_la_tarifa(self):
        a = _a(precio_unitario=72369, total=72369, precio_manual=72369)      # el caso de COT-000211
        self.assertEqual(a["origen"], "manual")
        self.assertEqual(a["margen_unit"], 22369)
        self.assertEqual(a["margen_pct_real"], 44.7)                          # no «30 %»
        self.assertEqual(a["precio_hoy"], 65000)                              # lo que habría sido el automático
        self.assertFalse(a["difiere_hoy"])                                    # un precio a mano no «cambia» con la tarifa

    def test_bajo_el_costo_es_perdida(self):
        a = _a(precio_unitario=35000, total=35000, precio_manual=35000, horas=2, valor_hh=20000)
        self.assertEqual(a["margen_unit"], -5000)
        self.assertEqual(a["margen_pct_real"], -12.5)

    def test_precio_fijo_no_tiene_costo_ni_margen(self):
        a = _a(precio_unitario=3500, total=35000, cantidad=10, tarifa_fijo=3500, horas=0, tecnicos=0)
        self.assertEqual(a["origen"], "fijo")
        self.assertIsNone(a["costo_unit"])
        self.assertIsNone(a["margen_total"])
        self.assertEqual(a["precio_hoy"], 3500)

    def test_la_tarifa_cambio_desde_que_se_cotizo(self):
        a = _a(precio_unitario=24000, total=24000, horas=1, valor_hh=20000)
        self.assertTrue(a["difiere_hoy"])
        self.assertEqual(a["precio_hoy"], 26000)

    def test_el_piso_se_reconoce(self):
        a = _a(precio_unitario=3500, total=3500, horas=0.1, tecnicos=1, valor_hh=20000, precio_piso=3500)     # costo 2000 × 1,3 = 2600 < piso
        self.assertEqual(a["origen"], "piso")
        self.assertEqual(a["precio_hoy"], 3500)

    def test_sin_tarifa_y_no_cobrable(self):
        self.assertEqual(_a(sin_tarifa=True, horas=None, tecnicos=None, valor_hh=None, precio_unitario=0, total=0)["origen"], "sin_tarifa")
        n = _a(no_cobrable=True, precio_unitario=0, total=0)
        self.assertEqual(n["origen"], "no_cobrable")
        self.assertIsNone(n["costo_unit"])

    def test_redondeo_mitad_hacia_arriba_como_el_cotizador(self):
        a = _a(horas=1, tecnicos=1, valor_hh=1, margen_pct=50, precio_unitario=2, total=2)                       # 1 × 1,5 = 1,5 -> 2
        self.assertEqual(a["precio_hoy"], 2)


class TestResumenYDudas(unittest.TestCase):
    def _items(self):
        return [
            {"total": 72369, "analisis": _a(precio_unitario=72369, total=72369, precio_manual=72369)},
            {"total": 35000, "analisis": _a(precio_unitario=35000, total=35000, precio_manual=35000, horas=2)},
            {"total": 35000, "analisis": _a(precio_unitario=3500, total=35000, cantidad=10, tarifa_fijo=3500, horas=0, tecnicos=0)},
            {"total": 0, "analisis": _a(no_cobrable=True, precio_unitario=0, total=0)},
        ]

    def test_resumen_solo_cuenta_lo_que_tiene_costo(self):
        r = cd.resumen(self._items())
        self.assertEqual((r["costo_total"], r["cobrado"], r["margen"]), (50000 + 40000, 72369 + 35000, 17369))
        self.assertEqual((r["items_con_costo"], r["items_sin_costo"], r["items_a_mano"], r["items_con_perdida"]), (2, 1, 2, 1))

    def test_las_dudas_usan_los_numeros_de_la_cotizacion(self):
        r = cd.resumen(self._items())
        d = dict(cd.dudas(r, tipo_servicio_label="Mantención", costo_ruta=23260))
        self.assertIn("$107.369", d["¿Cuánto margen deja la mano de obra?"])
        self.assertIn("$90.000", d["¿Cuánto margen deja la mano de obra?"])
        self.assertIn("2 ítems tienen el precio escrito a mano", d["¿Por qué un precio no calza con «costo + margen»?"])
        self.assertIn("Sí: 1 ítem", d["¿Hay ítems que se cobran bajo su costo?"])
        self.assertIn("$23.260", d["¿Y la ruta?"])
        self.assertIn("¿Por qué algunos ítems no muestran costo?", d)

    def test_ruta_excluida_y_sin_costo_en_ningun_item(self):
        r = cd.resumen([{"total": 3500, "analisis": _a(precio_unitario=3500, total=3500, tarifa_fijo=3500, horas=0, tecnicos=0)}])
        d = dict(cd.dudas(r, tipo_servicio_label="Mantención", ruta_excluida=True, costo_ruta=1000))
        self.assertIn("No se puede calcular", d["¿Cuánto margen deja la mano de obra?"])
        self.assertIn("EXCLUIDA", d["¿Y la ruta?"])


class TestPlantilla(unittest.TestCase):
    def _render(self, items):
        env = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")))
        env.filters["fkg"] = lambda v, n=2: ("%." + str(n) + "f") % float(v or 0)
        env.filters["chile_fmt"] = lambda v, f=None: "05/11/2026 12:01"
        for it in items:
            it["analisis"] = cd.analizar_item(
                cantidad=it["cantidad"], precio_unitario=it["precio_unitario"], total=it["total"], precio_manual=it.get("_pm"),
                tarifa_fijo=it.get("_fijo"), horas=it["horas"], tecnicos=it["tecnicos"], valor_hh=it["valor_hh"], margen_pct=30,
                sin_tarifa=it["sin_tarifa"])
        res = cd.resumen(items)
        cot = {"id": 1, "numero": "COT-000211", "empresa": "Kinesic Lab Spa", "rut": "7.736.988-3", "comuna": "Las Condes", "region": "RM",
               "tipo_servicio_label": "Mantención", "created_at": "x", "created_by": "Daniel", "subtotal_items": 72369, "costo_ruta": 23260,
               "ruta_excluida": False, "subtotal": 95629, "descuento_pct": 0, "descuento_monto": 0, "iva_pct": 19, "iva_monto": 18170, "total": 113799}
        return env.get_template("tickets/cotizacion_detalle_calculo.html").render(
            cot=cot, items=items, resumen=res, dudas=cd.dudas(res, tipo_servicio_label="Mantención", costo_ruta=23260), ruta=None,
            cfg={"margen_pct": 30}, valor_hh_vigente=20000, uf_info=None, uf_total=None, uf_emision=None, logo_b64=None, base_url=None)

    def _item(self, **kw):
        base = {"sku": "RE8026", "descripcion": "ILUS IS5", "clase_nombre": "Selectorizado", "cantidad": 1, "horas": 2.5, "tecnicos": 1, "hh": 2.5,
                "valor_hh": 20000, "costo_mo": 50000, "precio_unitario": 72369, "subtotal": 72369, "total": 72369, "sin_tarifa": False}
        base.update(kw)
        return base

    def test_un_precio_a_mano_se_dice_en_el_documento(self):
        h = self._render([self._item(_pm=72369)])
        self.assertIn("Precio escrito a mano", h)
        self.assertIn("El automático habría sido $65.000", h)
        self.assertIn("44.7 % sobre el costo", h)                     # el margen REAL, no «30 %»
        self.assertNotIn("costo + 30.0 %", h)                         # no se hace pasar por automático

    def test_hay_resumen_interno_y_dudas(self):
        h = self._render([self._item(_pm=72369)])
        for t in ("Resultado interno", "Para resolver dudas", "¿Cuánto margen deja la mano de obra?", "Margen real c/u", "Origen del precio"):
            self.assertIn(t, h)

    def test_un_precio_fijo_dice_sin_costo_registrado_no_cero(self):
        h = self._render([self._item(_fijo=3500, precio_unitario=3500, subtotal=3500, total=3500, horas=0, tecnicos=0, hh=0, cantidad=1)])
        self.assertIn("Precio fijo de la categoría", h)
        self.assertIn("sin costo registrado", h)

    def test_no_se_parten_filas_ni_cajas_y_el_toolbar_no_se_imprime(self):
        self.assertIn("page-break-inside: avoid; break-inside: avoid;", PLANTILLA)
        self.assertIn("table.items thead { display: table-header-group; }", PLANTILLA)
        self.assertIn(".no-print, .toolbar { display: none !important; }", PLANTILLA)
        self.assertNotIn("min-width: 880px;\n  }\n  table.items thead", PLANTILLA)         # ya no recorta columnas en el PDF
        i = PLANTILLA.index("@media (max-width: 900px)")
        self.assertIn("table.items { min-width: 880px; }", PLANTILLA[i:i + 500])         # el desplazamiento horizontal queda solo en pantalla angosta

    def test_boton_del_toolbar_de_44px_y_letra_minima(self):
        self.assertIn("min-height: 44px", PLANTILLA)


class TestMarcoComoLaOT(unittest.TestCase):
    """6-nov-2026 (Daniel: «no se apega para nada al diseño de la orden de trabajo»): el Detalle de cálculo lleva el MISMO marco que el PDF de la OT."""

    def _render(self, native):
        env = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")))
        env.filters["fkg"] = lambda v, n=2: ("%." + str(n) + "f") % float(v or 0)
        env.filters["chile_fmt"] = lambda v, f=None: "05/11/2026 12:01"
        it = {"sku": "RE8026", "descripcion": "ILUS IS5", "clase_nombre": "Selectorizado", "cantidad": 1, "horas": 2.5, "tecnicos": 1, "hh": 2.5,
              "valor_hh": 20000, "costo_mo": 50000, "precio_unitario": 65000, "subtotal": 65000, "total": 65000, "sin_tarifa": False}
        it["analisis"] = cd.analizar_item(cantidad=1, precio_unitario=65000, total=65000, horas=2.5, tecnicos=1, valor_hh=20000, margen_pct=30)
        res = cd.resumen([it])
        cot = {"id": 1, "numero": "COT-000211", "empresa": "Kinesic Lab Spa", "rut": "7.736.988-3", "comuna": "Las Condes", "region": "RM",
               "tipo_servicio_label": "Mantención", "created_at": "x", "created_by": "Daniel", "subtotal_items": 65000, "costo_ruta": 0,
               "ruta_excluida": False, "subtotal": 65000, "descuento_pct": 0, "descuento_monto": 0, "iva_pct": 19, "iva_monto": 12350, "total": 77350}
        return env.get_template("tickets/cotizacion_detalle_calculo.html").render(
            cot=cot, items=[it], resumen=res, dudas=cd.dudas(res, tipo_servicio_label="Mantención"), ruta=None, cfg={"margen_pct": 30},
            valor_hh_vigente=20000, uf_info=None, uf_total=None, uf_emision=None, logo_b64="AAAA", logo_shs_b64="BBBB", base_url=None,
            native_header_footer=native)

    def test_titulos_de_seccion_en_banda_negra_y_cajas_en_panel_gris(self):
        for t in ("--ilus-panel: #f0f0f0;", ".sec-title {", "background: var(--ilus-black); color: #fff;", ".info-card { background: var(--ilus-panel);",
                  "table.items thead th:first-child { border-top-left-radius: 6px; }"):
            self.assertIn(t, PLANTILLA)
        self.assertNotIn("h2.sec", PLANTILLA)                       # ya no son títulos sueltos con círculo numerado

    def test_en_pantalla_el_encabezado_va_en_el_documento_con_los_dos_logos(self):
        h = self._render(False)
        self.assertIn('class="head"', h)
        self.assertIn('class="logo-shs-box"', h)
        self.assertIn("DETALLE DE CÁLCULO", h)
        self.assertIn("N° COT-000211", h)
        self.assertIn('class="foot"', h)

    def test_en_el_pdf_el_encabezado_y_el_pie_son_los_nativos_y_el_del_documento_no_se_imprime(self):
        h = self._render(True)
        self.assertIn(".head, .head-sep, .foot { display: none !important; }", h)
        self.assertIn("margin: 30mm 0 14mm;", h)
        self.assertNotIn(".head, .head-sep, .foot { display: none !important; }", self._render(False))

    def test_el_margen_de_la_plantilla_calza_con_el_del_marco(self):
        import cot_detalle_pdf_marco as m
        self.assertIn(f"margin: {m.MARGEN_SUPERIOR_MM}mm 0 {m.MARGEN_INFERIOR_MM}mm;", self._render(True))

    def test_marco_nativo_trae_banda_negra_franja_roja_y_pie_con_paginas(self):
        import cot_detalle_pdf_marco as m
        e = m.encabezado_html("COT-000211", "Kinesic Lab Spa", logo_ilus_b64="AAAA", logo_shs_html="<b>SHS</b>")
        self.assertIn("background:#0a0a0a", e)
        self.assertIn("linear-gradient(90deg,#dc2626 0 25%,#0a0a0a 25% 100%)", e)
        self.assertIn("DETALLE DE CÁLCULO", e)
        self.assertIn("N&#176; COT-000211", e)
        self.assertIn("<b>SHS</b>", e)
        p = m.pie_html("COT-000211", "Kinesic Lab Spa")
        self.assertIn("border-top:1.5px solid #dc2626", p)
        self.assertIn("soportetec@sphs.cl", p)
        self.assertIn('class="pageNumber"', p)
        self.assertIn('class="totalPages"', p)

    def test_marco_escapa_el_texto_y_acorta_un_cliente_kilometrico(self):
        import cot_detalle_pdf_marco as m
        self.assertNotIn("<script>", m.encabezado_html("<script>x</script>", "y"))
        self.assertIn("…", m.pie_html("COT-1", "X" * 80))

    def test_la_ruta_del_pdf_usa_el_marco_y_el_logo_sphs_llega_a_la_plantilla(self):
        i = PY.index("def tk_cotizacion_detalle_calculo_pdf(cid):")
        f = PY[i:i + 3500]
        self.assertIn("native_header_footer=True", f)
        self.assertIn("header_template=_hdr, footer_template=_ftr", f)
        self.assertIn("_marco.MARGEN_SUPERIOR_MM", f)
        self.assertIn('"logo_shs_b64": _tk_cotiz_logo_shs_b64()', PY)


class TestServidor(unittest.TestCase):
    def test_el_servidor_manda_el_analisis_el_resumen_y_las_dudas(self):
        self.assertIn("import cot_detalle_calculo as _cotdet", PY)
        i = PY.index("def _tk_cotizacion_detalle_calculo_ctx(cid):")
        f = PY[i:i + 9000]
        self.assertIn("precio_unitario, subtotal, total, precio_manual", f)
        self.assertIn('"analisis": _cotdet.analizar_item(', f)
        self.assertIn("_res_dc = _cotdet.resumen(items)", f)
        self.assertIn('"dudas": _cotdet.dudas(', f)
        self.assertIn('"uf_emision":', f)
        self.assertIn("no_cobrable=_tk_cotiz_clase_no_cobrable(clase)", f)

    def test_sigue_siendo_solo_superadmin(self):
        for ruta in ('@app.route("/tickets/cotizaciones/<int:cid>/detalle-calculo")', '@app.route("/tickets/cotizaciones/<int:cid>/detalle-calculo/pdf")'):
            i = PY.index(ruta)
            self.assertIn("_tk_sin_cotizaciones_tecnico", PY[i:i + 250])
            self.assertIn("if not _tk_solo_superadmin():", PY[i:i + 500])


if __name__ == "__main__":
    unittest.main()

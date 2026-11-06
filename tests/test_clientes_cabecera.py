# -*- coding: utf-8 -*-
"""CABECERA «ENCHULADA» DE LA FICHA DEL CLIENTE (Daniel, 5-nov-2026): semáforo con su motivo a la vista, KPIs sin plata, acciones rápidas.

Tres capas, ninguna toca la base ni manda correos (REGLA #22):
  · el módulo puro `ficha_cliente_cabecera` (semáforo, motivos, KPIs) con una prueba por regla;
  · el cableado en app.py (`_mant_ficha_impl`) y la plantilla: texto verificado;
  · la cabecera RENDERIZADA con Jinja y medida en un navegador real a 390 / 820 / 1280 / 1366 / 1440 (REGLA #61).
Correr:  python -m pytest tests/test_clientes_cabecera.py -q -p no:warnings
"""
import datetime as dt
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import ficha_cliente_cabecera as F  # noqa: E402
from tests import _fuente_codigo as FC  # noqa: E402

HOY = dt.date(2026, 11, 5)
BASE = dict(hoy=HOY, cliente={"estado": "activo", "ejecutivo_nombre": "Aarón Rojas"}, contratos=[], ot_abiertas=0, ot_atrasadas=0,
            equipos_activos=4, equipos_total=4, equipos_con_problema=0, ult_visita=dt.date(2026, 10, 20), prox_visita=dt.date(2026, 11, 20),
            tiene_correo=True)


def armar(**kw):
    d = dict(BASE)
    d.update(kw)
    return F.armar(**d)


def textos(r):
    return [m["texto"] for m in r["semaforo"]["motivos"]]


class TestSemaforo(unittest.TestCase):
    def test_un_cliente_al_dia_sale_verde_y_sin_motivos(self):
        r = armar()
        self.assertEqual((r["semaforo"]["nivel"], r["semaforo"]["titulo"], r["semaforo"]["motivos"], r["info"]), ("ok", "Al día", [], []))

    def test_ot_atrasadas_es_rojo_y_dice_cuantas(self):
        r = armar(ot_abiertas=3, ot_atrasadas=2)
        self.assertEqual(r["semaforo"]["nivel"], "alerta")
        self.assertIn("2 OT atrasadas", textos(r)[0])
        self.assertIn("1 OT atrasada ", armar(ot_abiertas=1, ot_atrasadas=1)["semaforo"]["motivos"][0]["texto"])

    def test_contrato_vigente_con_fecha_pasada_es_rojo(self):
        r = armar(contratos=[{"nombre": "Plan Oro", "estado": "vigente", "fecha_vencimiento": HOY - dt.timedelta(days=5)}])
        self.assertEqual(r["semaforo"]["nivel"], "alerta")
        self.assertIn("venció hace 5 días", textos(r)[0])
        self.assertIn("Plan Oro", textos(r)[0])
        self.assertEqual(r["semaforo"]["motivos"][0]["accion"], "tab:contrato")

    def test_contrato_por_vencer_es_ambar_y_hoy_se_dice_hoy(self):
        r = armar(contratos=[{"nombre": "Plan", "estado": "vigente", "fecha_vencimiento": HOY + dt.timedelta(days=30)}])
        self.assertEqual((r["semaforo"]["nivel"], r["semaforo"]["titulo"]), ("aviso", "Hay cosas por revisar"))
        self.assertIn("vence en 30 días", textos(r)[0])
        self.assertIn("vence hoy", textos(armar(contratos=[{"nombre": "P", "estado": "vigente", "fecha_vencimiento": HOY}]))[0])
        self.assertEqual(armar(contratos=[{"nombre": "P", "estado": "vigente", "fecha_vencimiento": HOY + dt.timedelta(days=61)}])["semaforo"]["nivel"], "ok")

    def test_un_contrato_nuevo_reemplaza_al_viejo_que_quedo_marcado_vigente(self):
        r = armar(contratos=[{"nombre": "Viejo", "estado": "vigente", "fecha_vencimiento": HOY - dt.timedelta(days=200)},
                             {"nombre": "Nuevo", "estado": "vigente", "fecha_vencimiento": HOY + dt.timedelta(days=300)}])
        self.assertEqual(r["semaforo"]["nivel"], "ok")
        self.assertEqual(r["contrato"]["nombre"], "Nuevo")

    def test_contrato_indefinido_y_contenedor_de_documentos_no_molestan(self):
        self.assertEqual(armar(contratos=[{"nombre": "X", "estado": "indefinido", "es_indefinido": 1}])["semaforo"]["nivel"], "ok")
        self.assertEqual(armar(contratos=[{"nombre": "Contenedor de documentos", "estado": "vigente", "fecha_vencimiento": HOY - dt.timedelta(days=9)}])["semaforo"]["nivel"], "ok")

    def test_tuvo_contratos_pero_ninguno_vigente_es_solo_informacion(self):
        r = armar(contratos=[{"nombre": "Viejo", "estado": "vencido", "fecha_vencimiento": HOY - dt.timedelta(days=400)}])
        self.assertEqual(r["semaforo"]["nivel"], "ok")
        self.assertTrue(any("ninguno está vigente" in i["texto"] for i in r["info"]))

    def test_sin_visita_hace_mas_de_90_dias_y_nada_agendado_es_ambar(self):
        r = armar(ult_visita=HOY - dt.timedelta(days=120), prox_visita=None)
        self.assertEqual(r["semaforo"]["nivel"], "aviso")
        self.assertIn("120 días", textos(r)[0])
        self.assertIn("nada agendado", textos(r)[0])
        self.assertIn("Ninguna visita realizada", textos(armar(ult_visita=None, prox_visita=None))[0])

    def test_con_una_visita_agendada_o_sin_equipos_no_hay_aviso_de_visitas(self):
        self.assertEqual(armar(ult_visita=HOY - dt.timedelta(days=120))["semaforo"]["nivel"], "ok")
        self.assertEqual(armar(ult_visita=None, prox_visita=None, equipos_activos=0, equipos_total=0)["semaforo"]["nivel"], "ok")

    def test_equipos_con_problema_es_ambar(self):
        r = armar(equipos_con_problema=2)
        self.assertEqual(r["semaforo"]["nivel"], "aviso")
        self.assertIn("2 equipos con problema", textos(r)[0])
        self.assertIn("1 equipo con problema", textos(armar(equipos_con_problema=1))[0])

    def test_sin_correo_avisa_que_no_recibira_los_avisos_y_lleva_a_editar(self):
        r = armar(tiene_correo=False)
        self.assertEqual(r["semaforo"]["nivel"], "aviso")
        self.assertIn("no recibirá los avisos automáticos", textos(r)[0])
        self.assertEqual(r["semaforo"]["motivos"][0]["js"], "abrirEditarCliente()")

    def test_sin_ejecutivo_o_inactivo_es_informacion_y_no_cambia_el_color(self):
        r = armar(cliente={"estado": "inactivo", "ejecutivo_nombre": ""})
        self.assertEqual(r["semaforo"]["nivel"], "ok")
        self.assertEqual({i["texto"] for i in r["info"]}, {"Sin ejecutivo asignado", "Cliente inactivo"})

    def test_rojo_primero_y_gana_sobre_el_ambar(self):
        r = armar(ot_abiertas=2, ot_atrasadas=1, equipos_con_problema=3, tiene_correo=False)
        self.assertEqual(r["semaforo"]["nivel"], "alerta")
        self.assertEqual([m["nivel"] for m in r["semaforo"]["motivos"]], ["alerta", "aviso", "aviso"])

    def test_cada_motivo_trae_texto_y_accion_valida(self):
        r = armar(ot_abiertas=2, ot_atrasadas=1, equipos_con_problema=3, tiene_correo=False,
                  contratos=[{"nombre": "P", "estado": "vigente", "fecha_vencimiento": HOY + dt.timedelta(days=10)}],
                  ult_visita=None, prox_visita=None)
        for m in r["semaforo"]["motivos"] + r["info"]:
            self.assertTrue(m["texto"].strip())
            self.assertIn(m["accion"], F.ACCIONES_VALIDAS)
            self.assertTrue(m["js"], m["texto"])


class TestKpis(unittest.TestCase):
    def test_son_cinco_en_orden_y_sin_plata(self):
        r = armar(ot_abiertas=4, ot_atrasadas=1)
        self.assertEqual([k["clave"] for k in r["kpis"]], ["ot", "tickets", "equipos", "proxima", "ultima"])
        for k in r["kpis"]:
            blob = " ".join(str(v) for v in k.values()).lower()
            for plata in ("$", "monto", "costo", "precio", "margen", "factura", "proveedor", "deuda"):
                self.assertNotIn(plata, blob)

    def test_ot_abiertas_con_y_sin_atrasadas(self):
        k = armar(ot_abiertas=4, ot_atrasadas=1)["kpis"][0]
        self.assertEqual((k["valor"], k["detalle"], k["tono"]), ("4", "1 atrasada", "alerta"))
        k = armar(ot_abiertas=3, ot_atrasadas=0)["kpis"][0]
        self.assertEqual((k["valor"], k["detalle"], k["tono"]), ("3", "ninguna atrasada", "ok"))
        self.assertEqual(armar(ot_abiertas=0, ot_atrasadas=0)["kpis"][0]["detalle"], "sin trabajo pendiente")

    def test_si_no_se_pudo_leer_dice_guion_y_no_inventa_ni_pinta(self):
        r = armar(ot_abiertas=None, ot_atrasadas=None)
        k = r["kpis"][0]
        self.assertEqual((k["valor"], k["tono"]), ("—", "neutro"))
        self.assertEqual(r["semaforo"]["nivel"], "ok", "un dato que falta no pone el semáforo en rojo")

    def test_las_fechas_van_dia_mes_anio(self):
        r = armar(ult_visita=dt.datetime(2026, 10, 20, 15, 30), prox_visita="2026-11-20")
        k = {x["clave"]: x for x in r["kpis"]}
        self.assertEqual(k["ultima"]["valor"], "20/10/2026")
        self.assertEqual(k["ultima"]["detalle"], "hace 16 días")
        self.assertEqual(k["proxima"]["valor"], "20/11/2026")
        self.assertEqual(k["proxima"]["detalle"], "en 15 días")

    def test_proxima_visita_hoy_pasada_o_sin_agendar(self):
        self.assertEqual(armar(prox_visita=HOY)["kpis"][3]["detalle"], "hoy")
        k = armar(prox_visita=HOY - dt.timedelta(days=3))["kpis"][3]
        self.assertEqual((k["detalle"], k["tono"]), ("hace 3 días (sin reagendar)", "aviso"))
        k = armar(prox_visita=None)["kpis"][3]
        self.assertEqual((k["valor"], k["tono"]), ("Sin agendar", "aviso"))
        self.assertEqual(armar(prox_visita=None, equipos_activos=0)["kpis"][3]["tono"], "neutro")

    def test_ultima_visita_ninguna(self):
        k = armar(ult_visita=None)["kpis"][4]
        self.assertEqual((k["valor"], k["tono"]), ("Ninguna", "neutro"))

    def test_equipos_activos(self):
        k = armar(equipos_activos=4, equipos_total=6)["kpis"][2]
        self.assertEqual((k["valor"], k["detalle"], k["tono"]), ("4", "de 6 registrados", "ok"))
        k = armar(equipos_con_problema=2)["kpis"][2]
        self.assertEqual((k["detalle"], k["tono"]), ("2 con problema", "aviso"))

    def test_la_tarjeta_de_tickets_arranca_sin_dato_y_lleva_a_la_pestana(self):
        k = armar()["kpis"][1]
        self.assertEqual((k["valor"], k["tono"], k["js"]), ("—", "neutro", "switchTab('tickets');cargarTicketsFicha()"))


class TestAccionJs(unittest.TestCase):
    def test_lista_cerrada(self):
        self.assertEqual(F.accion_js("editar"), "abrirEditarCliente()")
        self.assertEqual(F.accion_js("tab:visitas"), "switchTab('visitas')")
        self.assertEqual(F.accion_js("tab:comunicaciones"), "switchTab('comunicaciones');cargarComunicaciones()")
        self.assertEqual(F.accion_js(""), "")

    def test_nada_raro_llega_al_html(self):
        for malo in ("tab:x');alert(1)//", "javascript:alert(1)", "tab:visitas');boom('", "tab:", "otra"):
            self.assertEqual(F.accion_js(malo), "", malo)

    def test_fecha_tolerante(self):
        self.assertEqual(F.fecha("2026-11-05 10:00:00"), HOY)
        self.assertEqual(F.fecha(dt.datetime(2026, 11, 5, 23, 59)), HOY)
        self.assertIsNone(F.fecha("no es fecha"))
        self.assertIsNone(F.fecha(None))
        self.assertEqual(F.fecha_txt(None), "")


class TestCableado(unittest.TestCase):
    APP = FC.texto("app.py")
    HTML = FC.texto("templates/mantenciones/ficha.html")

    def test_la_ficha_arma_la_cabecera_y_se_la_pasa_a_la_plantilla(self):
        i = self.APP.index("def _mant_ficha_impl(cid):")
        j = self.APP.index('@app.route("/mantenciones/api/clientes/<int:cid>", methods=["PUT"])', i)
        f = self.APP[i:j]
        self.assertIn("_fcc.armar(", f)
        self.assertIn("cli_cab   = _cli_cab", f)
        self.assertIn("_now_chile().date()", f, "la fecha de hoy debe ser la de Chile (UTC marcaría atrasadas las OT de hoy a las 21:00)")
        self.assertIn("import ficha_cliente_cabecera as _fcc", self.APP)

    def test_el_conteo_de_ot_es_de_solo_lectura_y_parametrizado(self):
        i = self.APP.index("# 🧭 2026-11-05 (Daniel: «ese módulo es viejo, quisiera enchularlo»)")
        bloque = self.APP[i:i + 3200]
        self.assertIn("FROM mant_visitas WHERE cliente_id=%s", bloque)
        self.assertIn("(_hoy_cab, cid)", bloque)
        for prohibido in ("mysql_execute", "INSERT ", "UPDATE ", "DELETE ", 'f"SELECT', "get_mysql"):
            self.assertNotIn(prohibido, bloque)

    def test_si_la_cabecera_falla_la_ficha_se_ve_igual(self):
        self.assertIn("{% if cli_cab %}", self.HTML)
        self.assertGreaterEqual(self.HTML.count("{% if cli_cab %}"), 2)

    def test_las_acciones_que_ya_existian_siguen_y_las_nuevas_abren_lo_mismo_que_antes(self):
        i = self.HTML.index('id="fcbAcciones"')
        bloque = self.HTML[i:i + 4200]
        for onclick in ("abrirEditarCliente()", "abrirNuevaVisita()", "abrirGenerarOT()", "abrirEmailManual()",
                        "/tickets/cotizaciones?desde_cliente={{ cliente.id }}", "/tickets?desde_cliente={{ cliente.id }}"):
            self.assertIn(onclick, bloque)
        self.assertIn("abrirEliminarCliente()", self.HTML, "el menú de eliminar (solo superadmin) no se quitó")
        self.assertIn("{% if not is_tecnico %}", bloque, "un técnico no ve acciones")

    def test_el_rut_va_con_el_filtro_rut_fmt(self):
        self.assertIn("{{ cliente.rut | rut_fmt }}", self.HTML)

    def test_sin_popups_nativos_ni_plata_en_la_franja_de_kpis(self):
        i = self.HTML.index('id="fcbKpis"')
        j = self.HTML.index("ALERTA GARANTÍA POR VENCER", i)
        franja = self.HTML[i:j]
        for malo in ("alert(", "confirm(", "prompt(", "monto", "costo", "margen", "precio"):
            self.assertNotIn(malo, franja)

    def test_el_css_cumple_la_regla_61(self):
        css = FC.texto("static/ficha_cabecera.css")
        self.assertIn("min-height: 44px", css)
        tams = [float(t) for t in re.findall(r"font-size:\s*([\d.]+)rem", css)]
        self.assertGreaterEqual(min(tams), 0.75)
        self.assertIn("@media (max-width: 767.98px)", css)


class TestNuevoTicketDesdeLaFicha(unittest.TestCase):
    """`/tickets?desde_cliente=<id>` (botón «Nuevo ticket» de la cabecera): static/tickets_list.js abre el modal con los datos de ESE cliente."""
    JS = FC.texto("static/tickets_list.js")
    INI = "// ── Ficha del cliente → «Nuevo ticket»"
    FIN = "}); // cierre de document.addEventListener('DOMContentLoaded', ...)"

    def bloque(self):
        i = self.JS.index(self.INI)
        return self.JS[i:self.JS.index(self.FIN, i)]

    def correr(self, search, respuestas):
        import json
        import shutil
        import subprocess
        if not shutil.which("node"):
            self.skipTest("Node no está instalado")
        codigo = r"""
const RESP = %s; const llamadas = [], toasts = [], propuestos = []; let mostrado = 0, reemplazado = 0;
global.location = { search: %s, pathname: '/tickets' };
global.history = { replaceState: () => { reemplazado++; } };
global.fetch = async (url) => { llamadas.push(url); const r = RESP.shift(); if (r === 'falla') throw new Error('red'); return { json: async () => r }; };
const ntModal = { show: () => { mostrado++; } };
function ntwProponerCliente(src, origen) { propuestos.push({ src, origen }); }
function ilusToast(m) { toasts.push(m); }
%s
setTimeout(() => console.log(JSON.stringify({ llamadas, toasts, propuestos, mostrado, reemplazado })), 50);
""" % (json.dumps(respuestas), json.dumps(search), self.bloque())
        r = subprocess.run(["node", "-e", codigo], capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertTrue(r.stdout.strip(), r.stderr[:400])
        return json.loads(r.stdout.strip().splitlines()[-1])

    def test_con_datos_abre_el_modal_y_propone_los_datos_del_cliente(self):
        d = self.correr("?desde_cliente=139", [
            {"ok": True, "cliente": {"id": 139, "razon_social": "Fit Paradise SpA", "rut": "76.123.456-K"}, "tickets": []},
            {"ok": True, "contactos": [{"nombre": "Carla Pérez", "tel": "+56 9 1234 5678", "email": "carla@fit.cl", "origen": "principal"}]}])
        self.assertEqual(d["llamadas"], ["/tickets/api/clientes-ficha/139/tickets?page=1&per_page=5", "/mantenciones/api/clientes/139/contactos"])
        self.assertEqual(d["mostrado"], 1)
        self.assertEqual(d["propuestos"], [{"origen": "ficha del cliente", "src": {
            "empresa": "Fit Paradise SpA", "rut": "76.123.456-K", "contacto": "Carla Pérez", "telefono": "+56 9 1234 5678", "email": "carla@fit.cl"}}])
        self.assertEqual(d["reemplazado"], 1, "recargar la página no debe volver a abrir el modal")

    def test_un_parametro_raro_no_hace_nada(self):
        for malo in ("?desde_cliente=abc", "?desde_cliente=1;DROP", "?desde_cliente=", "?otra=1", "?desde_cliente=12345678901"):
            d = self.correr(malo, [])
            self.assertEqual((d["llamadas"], d["mostrado"], d["propuestos"]), ([], 0, []), malo)

    def test_si_la_red_falla_el_modal_se_abre_igual_y_avisa(self):
        d = self.correr("?desde_cliente=139", ["falla", "falla"])
        self.assertEqual((d["mostrado"], d["propuestos"]), (1, []))
        self.assertEqual(len(d["toasts"]), 1)

    def test_si_solo_falla_una_de_las_dos_usa_lo_que_llego(self):
        d = self.correr("?desde_cliente=139", [{"ok": True, "cliente": {"razon_social": "Fit", "rut": "1-9"}}, "falla"])
        self.assertEqual(d["propuestos"][0]["src"]["empresa"], "Fit")
        self.assertEqual(d["propuestos"][0]["src"]["email"], "")

    def test_solo_usa_endpoints_que_ya_existian_y_no_escribe_nada(self):
        b = self.bloque()
        self.assertNotIn("method:", b)
        self.assertNotIn("POST", b)
        self.assertIn("ntwProponerCliente(", b, "el correo pasa por el flujo de chips: nunca se escribe solo")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  La cabecera renderizada, medida en un navegador real
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
def _cabecera_html(tecnico=False, **cab):
    """El bloque de la cabecera de ficha.html renderizado con Jinja y datos de mentira + la franja de KPIs/motivos."""
    import jinja2
    ini = TestCableado.HTML.index("{# ─────────────────────────────────────────────────────────\n   HEADER CLIENTE")
    fin = TestCableado.HTML.index("{# ══════════════════════════════════════════════════════════════════\n   ALERTA GARANTÍA POR VENCER")
    bloque = re.sub(r"\{#.*?#\}", "", TestCableado.HTML[ini:fin], flags=re.S)
    amb = {"re": re}
    FC.extraer(amb, "app.py", funcs=("_formato_rut_chile_impl", "_formato_rut_chile"))
    env = jinja2.Environment()
    env.filters["rut_fmt"] = amb["_formato_rut_chile"]
    cliente = {"id": 139, "razon_social": "Gimnasio Fit Paradise Sociedad por Acciones Limitada de Las Condes", "estado": "activo", "tipo_cliente": "mantencion",
               "rut": "76123456K", "ciudad": "Santiago", "comuna": "Las Condes", "contacto_nombre": "Carla Pérez González", "contacto_tel": "+56 9 1234 5678",
               "contacto_email": "carla.perez.gonzalez@gimnasiofitparadise.cl", "contacto2_nombre": "Luis Soto", "contacto2_tel": "+56 9 8765 4321",
               "contacto2_email": "luis.soto@gimnasiofitparadise.cl", "ejecutivo_nombre": "Aarón Rojas", "created_by": "Víctor", "created_at": dt.datetime(2025, 3, 1),
               "updated_by": "Aarón", "updated_at": dt.datetime(2026, 10, 30, 18, 5)}
    r = armar(ot_abiertas=5, ot_atrasadas=2, equipos_con_problema=2, tiene_correo=True, cliente=cliente,
              contratos=[{"nombre": "Plan Oro", "estado": "vigente", "fecha_vencimiento": HOY + dt.timedelta(days=20)}],
              ult_visita=HOY - dt.timedelta(days=100), prox_visita=None, **cab)
    return env.from_string(bloque).render(cliente=cliente, cli_cab=r, is_tecnico=tecnico, permissions={"superadmin": not tecnico})


def _pagina(tecnico=False):
    css = FC.texto("static/mantenciones_ficha.css") + "\n" + FC.texto("static/ficha_cabecera.css")
    util = (".d-flex{display:flex}.flex-wrap{flex-wrap:wrap}.flex-shrink-0{flex-shrink:0}.gap-2{gap:.5rem}.gap-3{gap:1rem}.align-items-start{align-items:flex-start}"
            ".align-items-center{align-items:center}.mb-0{margin-bottom:0}.mb-1{margin-bottom:.25rem}.me-1{margin-right:.25rem}.me-2{margin-right:.5rem}"
            ".btn{display:inline-block;padding:.375rem .75rem;font-size:1rem;border:1px solid transparent;border-radius:.375rem;cursor:pointer;background:none}"
            ".btn-sm{padding:.25rem .5rem;font-size:.875rem}.btn-ilus{background:#dc2626;color:#fff}.btn-outline-secondary{border-color:#6c757d;color:#6c757d}"
            ".dropdown{position:relative}.dropdown-menu{display:none}.small{font-size:.875em}.text-muted{color:#6c757d}.dropdown-toggle::after{content:'▾'}")
    return ("<!doctype html><html lang='es'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<style>:root{--border:#e5e7eb;--rad:12px;--ilus-red:#dc2626}*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;font-size:14px}"
            ".main{max-width:1180px;margin:0 auto;padding:12px}" + util + "</style><style>" + css + "</style></head><body><div class='main'>"
            + _cabecera_html(tecnico) + "</div></body></html>")


try:
    from playwright.sync_api import sync_playwright
except Exception:                      # pragma: no cover
    sync_playwright = None

ANCHOS = (390, 820, 1280, 1366, 1440)


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.html = {False: _pagina(False), True: _pagina(True)}
        cls.actual = False
        cls.ctx = cls.nav.new_context(viewport={"width": 1280, "height": 900})
        cls.ctx.set_default_timeout(8000)
        cls.ctx.route("https://cab.test/**", lambda r: r.fulfill(status=200, content_type="text/html; charset=utf-8", body=cls.html[cls.actual]))
        cls.pg = cls.ctx.new_page()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.ctx.close()
            cls.nav.close()
        finally:
            cls._pw.stop()

    def abrir(self, ancho, tecnico=False):
        type(self).actual = tecnico
        self.pg.set_viewport_size({"width": ancho, "height": 900})
        self.pg.goto("https://cab.test/c")
        return self.pg

    def test_se_ve_todo_completo_sin_truncar(self):
        pg = self.abrir(1280)
        cuerpo = pg.inner_text(".cli-header")
        for esperado in ("Gimnasio Fit Paradise Sociedad por Acciones Limitada de Las Condes", "76.123.456-K", "Aarón Rojas", "2.º contacto",
                         "luis.soto@gimnasiofitparadise.cl", "Requiere atención", "2 OT atrasadas", "Sin visita hace 100 días y nada agendado",
                         "El contrato vence en 20 días", "2 equipos con problema", "Sin agendar", "28/07/2026", "Mantención"):
            self.assertIn(esperado, cuerpo)
        etiquetas = [e.lower() for e in pg.locator(".fcb-kpi-l").all_inner_texts()]        # el CSS las pone en mayúsculas: se compara sin mayúsculas
        self.assertEqual(etiquetas, ["ot abiertas", "tickets abiertos", "equipos activos", "próxima visita", "última visita"])
        for boton in ("Generar OT", "Nueva visita", "Nueva cotización", "Nuevo ticket", "Enviar email", "Editar cliente"):
            self.assertGreaterEqual(pg.get_by_role("button", name=boton).count(), 1, boton)

    def test_un_tecnico_ve_los_kpis_pero_ninguna_accion(self):
        pg = self.abrir(1280, tecnico=True)
        self.assertEqual(pg.locator(".fcb-btn").count(), 0)
        self.assertEqual(pg.locator(".fcb-kpi").count(), 5)
        self.assertEqual(pg.locator("#fcbSemaforo").count(), 1)

    def test_medidas_regla_61_en_cinco_anchos(self):
        for ancho in ANCHOS:
            for tecnico in (False, True):
                pg = self.abrir(ancho, tecnico)
                m = pg.evaluate("""() => {
                  const de = document.documentElement, out = {scroll: de.scrollWidth - de.clientWidth, bajos: [], chicas: [], recortes: [], fuera: []};
                  const hdr = document.querySelector('.cli-header').getBoundingClientRect();
                  document.querySelectorAll('.fcb-btn, .fcb-kpi-btn, .fcb-chip, .fcb-acciones .dropdown > .btn, .fcb-sem').forEach(el => {
                    const r = el.getBoundingClientRect(); if (!r.height) return;
                    if (r.height < 43.5 && !el.classList.contains('fcb-sem')) out.bajos.push(el.className + ' ' + Math.round(r.height));   // el semáforo es un estado, no un botón
                    if (r.right > hdr.right + 1 || r.left < hdr.left - 1) out.fuera.push(el.className);
                  });
                  const w = document.createTreeWalker(document.querySelector('.cli-header'), NodeFilter.SHOW_TEXT);
                  while (w.nextNode()) { const t = w.currentNode; if (!t.textContent.trim()) continue;
                    const el = t.parentElement; if (!el || el.closest('.cli-avatar')) continue;
                    const fs = parseFloat(getComputedStyle(el).fontSize); if (fs < 11.99) out.chicas.push(t.textContent.trim().slice(0, 28) + ' ' + fs); }
                  document.querySelectorAll('.fcb-kpi-v, .fcb-kpi-d, .fcb-chip, .fcb-kpi-l').forEach(el => { if (el.scrollWidth > el.clientWidth + 1) out.recortes.push(el.textContent.trim().slice(0, 30)); });
                  return out; }""")
                etiqueta = f"{ancho}px {'técnico' if tecnico else 'gestión'}"
                self.assertLessEqual(m["scroll"], 0, f"scroll de lado a {etiqueta}")
                self.assertEqual(m["bajos"], [], f"objetivos de menos de 44 px a {etiqueta}")
                self.assertEqual(m["chicas"], [], f"letra menor a 12 px a {etiqueta}")
                self.assertEqual(m["recortes"], [], f"texto recortado a {etiqueta}")
                self.assertEqual(m["fuera"], [], f"algo se sale de la cabecera a {etiqueta}")

    def test_cinco_tarjetas_en_escritorio_tres_en_tableta_dos_en_celular(self):
        def columnas():
            return self.pg.evaluate("getComputedStyle(document.querySelector('.fcb-kpis')).gridTemplateColumns.split(' ').length")
        self.abrir(1280); self.assertEqual(columnas(), 5)
        self.abrir(820);  self.assertEqual(columnas(), 3)
        self.abrir(390);  self.assertEqual(columnas(), 2)


if __name__ == "__main__":
    unittest.main()

"""🚚 Logística de las guías en un navegador de verdad (Chromium por Playwright), 29-oct-2026.

Página estática con datos de MENTIRA (nunca producción, nunca Check ni Random reales): la tarjeta «Trazabilidad en Random»
con dos guías, el chip «N diferencias con la OT», el semáforo de cada guía con lo que dijo Check y el detalle al hacer clic.
Se mira en escritorio (1280) y celular (375): sin errores de consola, sin scroll horizontal, nada cortado (REGLA #15) y,
como pidió Daniel («todo bien resumido, no tarjetas tan grandes»), la guía no se pinta entera: solo el semáforo.
Si ILUS_CAPTURAS_DIR está definida, deja ahí las capturas. Correr con:
  python -m unittest tests.test_ot_guia_logistica_navegador
"""
import json
import os
import pathlib
import tempfile
import unittest

RAIZ = pathlib.Path(__file__).resolve().parent.parent

try:
    from playwright.sync_api import sync_playwright
except Exception:          # pragma: no cover - sin Playwright en esta máquina
    sync_playwright = None

NOMBRE_LARGO = "Booty Builder Cushioned Belt Pro Edición Gimnasio Comercial 2026 con cinturón acolchado (negro)"
CADENA = {"ok": True, "visita_id": 171, "sugerencias": [], "alertas": [], "leido": True, "cadenas": [{"leida": True, "nodos": [
    {"clave": ["VD", "10414"], "tipo": "VD", "numero": "10414", "titulo": "VD 10414", "tipo_txt": "Nota de venta", "fecha": "06/10/2026",
     "en_ot": True, "dada_de_baja": True, "reemplazado_por": "FCV 11745", "saldo": None, "estado_saldo": "sin_servicios", "usos": []},
    {"clave": ["FCV", "11745"], "tipo": "FCV", "numero": "11745", "titulo": "FCV 11745", "tipo_txt": "Factura", "fecha": "06/10/2026",
     "en_ot": True, "saldo": None, "estado_saldo": "sin_servicios", "usos": []},
    {"clave": ["GDV", "33193"], "tipo": "GDV", "numero": "33193", "titulo": "GDV 33193", "tipo_txt": "Guía de despacho",
     "fecha": "08/10/2026", "en_ot": False, "desde": "FCV 11745", "lineas": 1, "texto": "Salió el stock el 08/10/2026 (1 producto)",
     "productos": [{"sku": "BB-01", "nombre": NOMBRE_LARGO, "cantidad": 1}]},
    {"clave": ["GDV", "33194"], "tipo": "GDV", "numero": "33194", "titulo": "GDV 33194", "tipo_txt": "Guía de despacho",
     "fecha": "09/10/2026", "en_ot": False, "desde": "FCV 11745", "lineas": 1, "texto": "Salió el stock el 09/10/2026 (1 producto)",
     "productos": [{"sku": "MT-T70", "nombre": "Trotadora Matrix T70 con consola XR y monitor de frecuencia cardíaca", "cantidad": 2}]}]}]}

COMP = {"estado": "diferencias", "color": "ambar", "n_diferencias": 1, "motivo": None,
        "veredicto": "Las guías GDV 33193 y GDV 33194 sacaron 1 × " + NOMBRE_LARGO + " y 2 × Trotadora Matrix T70 con consola XR y "
                     "monitor de frecuencia cardíaca; la OT gestiona 4 equipos: falta por salir 1 (Bicicleta de spinning Keiser M3i "
                     "con pantalla y soporte para teléfono).",
        "productos": [], "anuladas": [], "sin_sku": [], "equipos_del_cliente": [], "guias": ["33193", "33194"],
        "por_guia": {
            "33193": {"numero": "33193", "color": "ambar", "n_dif": 0, "texto": "", "lineas": [
                {"sku": "BB-01", "nombre": NOMBRE_LARGO, "cantidad": 1, "estado": "coincide", "estado_txt": "Coincide",
                 "es_diferencia": False, "texto": "Salió 1 y la OT gestiona 1."}]},
            "33194": {"numero": "33194", "color": "verde", "n_dif": 0, "texto": "", "lineas": [
                {"sku": "MT-T70", "nombre": "Trotadora Matrix T70 con consola XR y monitor de frecuencia cardíaca", "cantidad": 2,
                 "estado": "coincide", "estado_txt": "Coincide", "es_diferencia": False, "texto": "Salió 2 y la OT gestiona 2."}]}},
        "no_salio": [{"sku": "KM3I", "nombre": "Bicicleta de spinning Keiser M3i con pantalla y soporte para teléfono", "gestiona": 1,
                      "estado": "esta_en_la_OT_y_no_salio", "estado_txt": "Está en la OT y no salió", "es_diferencia": True,
                      "texto": "La OT gestiona 1 y no salió en ninguna guía."}]}
RESUMEN = {"ok": True, "visita_id": 171, "check": True, "comparacion": COMP, "guias": [], "semaforos": {
    "33193": {"color": "ambar", "texto": "Falta por salir 1 de lo que gestiona la OT · consultando la bodega (Check)…", "pendiente": True},
    "33194": {"color": "gris", "texto": "Salió lo que gestiona la OT · consultando la bodega (Check)…", "pendiente": True}}}
CON_CHECK = dict(RESUMEN, semaforos={
    "33193": {"color": "ambar", "texto": "Falta por salir 1 de lo que gestiona la OT · bodega expidió 1 de 1", "pendiente": False},
    "33194": {"color": "verde", "texto": "Salió todo lo gestionado · bodega expidió 2 de 2", "pendiente": False}})
DETALLE = dict(RESUMEN, semaforos={"33193": CON_CHECK["semaforos"]["33193"]}, bodega={"33193": {
    "estado": "ok", "color": "verde", "texto": "Bodega expidió todo: 1 de 1 unidad.", "fuente": "GDV 33193", "fuente_es_guia": True,
    "emision": "08/10/2026", "entrega": "09/10/2026",
    "etapas": [{"clave": "pedido", "titulo": "", "hechas": 1, "de": 1, "completa": True},
               {"clave": "pickeado", "titulo": "", "hechas": 1, "de": 1, "completa": True},
               {"clave": "despachado", "titulo": "", "hechas": 1, "de": 1, "completa": True}],
    "guia_lineas": [dict(COMP["por_guia"]["33193"]["lineas"][0], bodega={"pedidas": 1, "pickeadas": 1, "despachadas": 1})],
    "movimientos": {"estado": "ok", "edad_min": 7, "ots": [
        {"doc": "GDV 33193", "ot": "4567", "tipo": "PICKING MATERIAL", "estado": "FINALIZADA", "inicio": "08/10/2026 10:20",
         "fin": "08/10/2026 10:45", "personas": ["Luis Alberto Pérez Contreras"], "uas": ["UA1012965", "UA1012966"]},
        {"doc": "GDV 33193", "ot": "4571", "tipo": "CONTROL DE SALIDA", "estado": "FINALIZADA", "inicio": "08/10/2026 11:02",
         "fin": "", "personas": ["Samuel Rojas"], "uas": []}]}}})

PAGINA = """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trazabilidad (datos de mentira)</title>
<style>body{margin:0;padding:16px;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#f3f4f6;color:#111827}
.fm{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:12px;max-width:1100px;margin:0 auto;display:flex;flex-direction:column;gap:10px}
.fm-top{font-weight:900}</style>
<link rel="stylesheet" href="%(css1)s"><link rel="stylesheet" href="%(css2)s">
<script>
const RESP = %(resp)s;
window.fetch = async (u) => { await new Promise(r => setTimeout(r, 30)); const r = RESP[u];
  return { ok: !!r, json: async () => r }; };
window.ilusToast = function () {};
</script>
<script src="%(js1)s" defer></script><script src="%(js2)s" defer></script>
</head><body><div class="fm" data-fin-motor data-vid="171"><div class="fm-top">Costos y documentación (datos de mentira)</div></div></body></html>"""


@unittest.skipUnless(sync_playwright, "Playwright no está instalado")
class TestNavegador(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="glogi_")
        resp = {"/ot/api/171/documentos/cadena": CADENA, "/ot/api/171/documentos/logistica": RESUMEN,
                "/ot/api/171/documentos/logistica?check=1": CON_CHECK, "/ot/api/171/documentos/logistica?guia=33193": DETALLE}
        est = RAIZ / "static"
        html = PAGINA % {"css1": (est / "ot_doc_cadena.css").as_uri(), "css2": (est / "ot_guia_logistica.css").as_uri(),
                         "js1": (est / "ot_doc_cadena.js").as_uri(), "js2": (est / "ot_guia_logistica.js").as_uri(),
                         "resp": json.dumps(resp, ensure_ascii=False)}
        cls.ruta = pathlib.Path(cls.dir) / "trazabilidad.html"
        cls.ruta.write_text(html, encoding="utf-8")
        cls.capturas = os.environ.get("ILUS_CAPTURAS_DIR") or cls.dir

    def _revisar(self, ancho, alto, nombre):
        errores = []
        with sync_playwright() as p:
            nav = p.chromium.launch()
            pag = nav.new_page(viewport={"width": ancho, "height": alto})
            pag.on("console", lambda m: errores.append(m.text) if m.type == "error" else None)
            pag.on("pageerror", lambda e: errores.append(str(e)))
            pag.goto(self.ruta.as_uri())
            pag.wait_for_selector(".gl-sem:not(.pend)", timeout=10000)
            sem = pag.eval_on_selector_all("[data-gl-estado]", "els => els.map(e => e.innerText.trim())")
            chip = pag.inner_text("[data-gl-chip]")
            fondo_guia = pag.eval_on_selector_all(".dc-n.t-guia", "els => els.map(e => getComputedStyle(e).backgroundColor)")
            pag.screenshot(path=os.path.join(self.capturas, f"logistica_{nombre}_resumen.png"), full_page=True)
            pag.click("[data-gl-ver='33193']")
            pag.wait_for_selector(".gl-det", timeout=10000)
            detalle = pag.inner_text(".gl-det")
            medidas = pag.evaluate("""() => {
              const doc = document.documentElement;
              const cortados = [];
              document.querySelectorAll('.dc *').forEach(el => {
                if (el.classList.contains('gl-sr')) return;          // texto solo para el lector de pantalla (1 px a propósito)
                const cs = getComputedStyle(el);
                if (cs.textOverflow === 'ellipsis' || cs.webkitLineClamp !== 'none' && cs.webkitLineClamp) cortados.push(el.className + ':clamp');
                if (el.children.length === 0 && el.innerText && el.scrollWidth > el.clientWidth + 1 && cs.overflowX !== 'visible')
                  cortados.push((el.className || el.tagName) + ':' + el.scrollWidth + '>' + el.clientWidth);
              });
              const dot = document.querySelector('.gl-dot').getBoundingClientRect();
              return { scroll: doc.scrollWidth, ancho: window.innerWidth, cortados, dot: [dot.width, dot.height],
                       btn: document.querySelector('[data-gl-ver]').getBoundingClientRect().height };
            }""")
            pag.screenshot(path=os.path.join(self.capturas, f"logistica_{nombre}_detalle.png"), full_page=True)
            nav.close()
        self.assertEqual(errores, [], errores)
        self.assertLessEqual(medidas["scroll"], medidas["ancho"], "sin scroll horizontal")
        self.assertEqual(medidas["cortados"], [], "nada truncado (REGLA #15)")
        self.assertEqual(medidas["dot"], [10, 10], "semáforo chico")
        self.assertIn("1 diferencia con la OT", chip)
        self.assertIn("bodega expidió 1 de 1", sem[0])
        self.assertIn("Salió todo lo gestionado", sem[1])
        self.assertTrue(all(f in ("rgb(255, 255, 255)", "rgba(0, 0, 0, 0)") for f in fondo_guia), fondo_guia)
        self.assertIn(NOMBRE_LARGO, detalle, "nombre completo")
        self.assertIn("Luis Alberto Pérez Contreras", detalle)
        self.assertIn("Bicicleta de spinning Keiser M3i con pantalla y soporte para teléfono", detalle)
        return medidas

    def test_escritorio_1280(self):
        m = self._revisar(1280, 900, "1280")
        self.assertGreaterEqual(m["btn"], 30)

    def test_celular_375(self):
        m = self._revisar(375, 812, "375")
        self.assertGreaterEqual(m["btn"], 44, "botón táctil de 44 px en el celular (REGLA #3)")


if __name__ == "__main__":
    unittest.main()

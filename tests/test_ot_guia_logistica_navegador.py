"""🚚 Logística de las guías en un navegador de verdad (Chromium por Playwright), 29-oct-2026.

Página estática con datos de MENTIRA (nunca producción, nunca Check ni Random reales): la tarjeta «Trazabilidad en Random»
COMPACTA (Daniel, 29-oct tarde: «quiero que sea PEQUEÑA; no quiero que ocupe tanto espacio») — una fila de chips por venta,
un punto de semáforo por guía (verde · ámbar · gris), la línea con lo ámbar, el chip «N diferencias con la OT» y, al tocar una
guía, el panel con las diferencias con la OT ARRIBA, «qué salió y qué no» (OT · guía · Check) y los movimientos de bodega.
Al tocar la factura, su detalle de siempre (saldo, otras OT): nada se perdió (REGLA #4.2).
Se mira en escritorio (1280) y celular (375): sin errores de consola, sin scroll horizontal, nada cortado (REGLA #15), toques
de 44 px en el celular (REGLA #3) y la guía no se pinta entera: solo el punto.
Si ILUS_CAPTURAS_DIR está definida, deja ahí las capturas (prefijo logistica_v2_). Correr con:
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
TROTA = "Trotadora Matrix T70 con consola XR y monitor de frecuencia cardíaca"
BICI = "Bicicleta de spinning Keiser M3i con pantalla y soporte para teléfono"
REMO = "Remo Concept2 RowErg modelo D con monitor PM5"
BANCO = "Banco ajustable multiposición FID 45° a 90° con rodillos para piernas"


def _gdv(num, fecha, prods):
    return {"clave": ["GDV", num], "tipo": "GDV", "numero": num, "titulo": "GDV " + num, "tipo_txt": "Guía de despacho",
            "fecha": fecha, "en_ot": False, "desde": "FCV 11745", "lineas": len(prods), "unidades": sum(q for _s, _n, q in prods),
            "texto": "Salió el stock el %s (%d producto%s)" % (fecha, len(prods), "" if len(prods) == 1 else "s"),
            "productos": [{"sku": s, "nombre": n, "cantidad": q} for s, n, q in prods]}


CADENA = {"ok": True, "visita_id": 171, "sugerencias": [], "alertas": [], "leido": True, "cadenas": [{"leida": True, "nodos": [
    {"clave": ["VD", "10414"], "tipo": "VD", "numero": "10414", "titulo": "VD 10414", "tipo_txt": "Nota de venta", "fecha": "06/10/2026",
     "en_ot": True, "dada_de_baja": True, "reemplazado_por": "FCV 11745", "saldo": None, "estado_saldo": "sin_servicios", "usos": []},
    {"clave": ["FCV", "11745"], "tipo": "FCV", "numero": "11745", "titulo": "FCV 11745", "tipo_txt": "Factura", "fecha": "06/10/2026",
     "en_ot": True, "mismo_cliente": True, "estado_saldo": "parcial",
     "saldo": {"servicio": {"total": 961640, "usado": 252101, "saldo": 709539}, "despacho": {"total": 450000, "usado": 0, "saldo": 450000}},
     "usos": [{"vid": 249, "numero_ot": "OT-2026-00249", "cliente": "Physica Ppc Centro de Entrenamiento", "url": "/ot/249",
               "servicio": 252101, "despacho": 0, "medible": True, "margen_pct": 24.6, "nivel": "verde"}]},
    _gdv("33193", "08/10/2026", [("BB-01", NOMBRE_LARGO, 1)]),
    _gdv("33194", "09/10/2026", [("MT-T70", TROTA, 2)]),
    _gdv("33195", "09/10/2026", [("C2-D", REMO, 1)]),
    _gdv("33196", "27/10/2026", [("FID-45", BANCO, 1)])]}]}


def _linea(sku, nombre, q, estado="coincide", dif=False, texto=""):
    return {"sku": sku, "nombre": nombre, "cantidad": q, "estado": estado, "estado_txt": "Coincide" if estado == "coincide" else "x",
            "es_diferencia": dif, "texto": texto or ("Salió %d y la OT gestiona %d." % (q, q))}


COMP = {"estado": "diferencias", "color": "ambar", "n_diferencias": 1, "motivo": None,
        "veredicto": "Las guías sacaron lo que gestiona la OT, salvo 1 (" + BICI + ").",
        "productos": [], "anuladas": [], "sin_sku": [], "equipos_del_cliente": [], "guias": ["33193", "33194", "33195", "33196"],
        "por_guia": {"33193": {"numero": "33193", "color": "ambar", "n_dif": 0, "texto": "", "lineas": [_linea("BB-01", NOMBRE_LARGO, 1)]},
                     "33194": {"numero": "33194", "color": "verde", "n_dif": 0, "texto": "", "lineas": [_linea("MT-T70", TROTA, 2)]},
                     "33195": {"numero": "33195", "color": "verde", "n_dif": 0, "texto": "", "lineas": [_linea("C2-D", REMO, 1)]},
                     "33196": {"numero": "33196", "color": "verde", "n_dif": 0, "texto": "", "lineas": [_linea("FID-45", BANCO, 1)]}},
        "no_salio": [{"sku": "KM3I", "nombre": BICI, "gestiona": 1, "estado": "esta_en_la_OT_y_no_salio",
                      "estado_txt": "Está en la OT y no salió", "es_diferencia": True, "texto": "La OT gestiona 1 y no salió en ninguna guía."}]}
GUIAS = [{"numero": n, "fecha": f, "desde": "FCV 11745", "anulada": False, "lineas": []}
         for n, f in (("33193", "08/10/2026"), ("33194", "09/10/2026"), ("33195", "09/10/2026"), ("33196", "27/10/2026"))]
PEND = "consultando la bodega (Check)…"
RESUMEN = {"ok": True, "visita_id": 171, "check": True, "comparacion": COMP, "guias": GUIAS, "semaforos": {
    "33193": {"color": "ambar", "texto": "Falta por salir 1 de lo que gestiona la OT · " + PEND, "pendiente": True},
    "33194": {"color": "gris", "texto": "La guía cuadra con la OT · " + PEND, "pendiente": True},
    "33195": {"color": "gris", "texto": "La guía cuadra con la OT · " + PEND, "pendiente": True},
    "33196": {"color": "gris", "texto": "La guía cuadra con la OT · " + PEND, "pendiente": True}}}
B_TEXTO = TROTA + " (MT-T70): la OT gestiona 2; Check informa que salió 1."
CON_CHECK = dict(RESUMEN, check_ot={"modo": "por_producto", "gracia_dias": 3,
                                    "hallazgos": [{"codigo": "check_expidio_menos", "texto": B_TEXTO}]}, semaforos={
    "33193": {"color": "ambar", "texto": "Falta por salir 1 de lo que gestiona la OT · bodega expidió 1 de 1", "pendiente": False,
              "cifras": {"ot": 2, "guia": 1, "check": 1}},
    "33194": {"color": "ambar", "texto": "La guía cuadra con la OT · bodega expidió 1 de 2", "pendiente": False,
              "cifras": {"ot": 2, "guia": 2, "check": 1}},
    "33195": {"color": "verde", "texto": "Salió todo lo gestionado · bodega expidió 1 de 1", "pendiente": False,
              "cifras": {"ot": 1, "guia": 1, "check": 1}},
    "33196": {"color": "gris", "texto": "La guía cuadra con la OT · sin información de bodega (Check)", "pendiente": False,
              "cifras": {"ot": 1, "guia": 1, "check": None}}})
DETALLE = dict(CON_CHECK, bodega={"33194": {
    "estado": "ok", "color": "ambar", "texto": "Bodega expidió 1 de 2 unidades.", "fuente": "GDV 33194", "fuente_es_guia": True,
    "emision": "09/10/2026", "entrega": "10/10/2026",
    "etapas": [{"clave": "pedido", "titulo": "", "hechas": 2, "de": 2, "completa": True},
               {"clave": "pickeado", "titulo": "", "hechas": 2, "de": 2, "completa": True},
               {"clave": "despachado", "titulo": "", "hechas": 1, "de": 2, "completa": False}],
    "movimientos": {"estado": "ok", "edad_min": 7, "ots": [
        {"doc": "GDV 33194", "ot": "4567", "tipo": "PICKING MATERIAL", "estado": "FINALIZADA", "inicio": "09/10/2026 10:20",
         "fin": "09/10/2026 10:45", "personas": ["Luis Alberto Pérez Contreras"], "uas": ["UA1012965", "UA1012966"]},
        {"doc": "GDV 33194", "ot": "4571", "tipo": "CONTROL DE SALIDA", "estado": "FINALIZADA", "inicio": "09/10/2026 11:02",
         "fin": "", "personas": ["Samuel Rojas"], "uas": []}]}}},
    detalle={"guia": "33194", "check": {"estado": "parcial", "color": "ambar", "texto": "bodega expidió 1 de 2"},
             "diferencias": [{"codigo": "check_expidio_menos", "regla": "B", "texto": B_TEXTO},
                             {"codigo": "esta_en_la_OT_y_no_salio", "regla": "A",
                              "texto": BICI + " (KM3I): la OT gestiona 1 y no salió en ninguna guía."}],
             "productos": [{"sku": "MT-T70", "nombre": TROTA, "ot": 2, "guia": 2, "check": 1, "estado": "coincide",
                            "es_diferencia": False, "texto": "Salió 2 y la OT gestiona 2.", "check_txt": "Según Check salió 1 de 2",
                            "check_color": "ambar", "color": "ambar", "en_guia": True},
                           {"sku": "KM3I", "nombre": BICI, "ot": 1, "guia": 0, "check": None, "estado": "esta_en_la_OT_y_no_salio",
                            "es_diferencia": True, "texto": "La OT gestiona 1 y no salió en ninguna guía.",
                            "check_txt": "No salió en ninguna guía", "check_color": "gris", "color": "ambar", "en_guia": False}]})

PAGINA = """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trazabilidad (datos de mentira)</title>
<link rel="stylesheet" href="%(iconos)s">
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

MEDIDAS = """() => {
  const doc = document.documentElement;
  const cortados = [];
  document.querySelectorAll('.dc *').forEach(el => {
    if (el.classList.contains('gl-sr')) return;          // texto solo para el lector de pantalla (1 px a propósito)
    const cs = getComputedStyle(el);
    if (cs.textOverflow === 'ellipsis' || cs.webkitLineClamp !== 'none' && cs.webkitLineClamp) cortados.push(el.className + ':clamp');
    if (el.children.length === 0 && el.innerText && el.scrollWidth > el.clientWidth + 1 && cs.overflowX !== 'visible')
      cortados.push((el.className || el.tagName) + ':' + el.scrollWidth + '>' + el.clientWidth);
  });
  const fuera = [...document.querySelectorAll('.dc *')].filter(el => {
    const r = el.getBoundingClientRect(); return r.width && (r.right > window.innerWidth + 1 || r.left < -1); }).map(e => e.className);
  const dot = document.querySelector('[data-gl-dot]').getBoundingClientRect();
  const chips = [...document.querySelectorAll('.dc-doc')].map(b => Math.round(b.getBoundingClientRect().height));
  return { scroll: doc.scrollWidth, ancho: window.innerWidth, cortados, fuera, dot: [dot.width, dot.height], chips,
           alto_tarjeta: Math.round(document.querySelector('.dc').getBoundingClientRect().height) };
}"""


@unittest.skipUnless(sync_playwright, "Playwright no está instalado")
class TestNavegador(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="glogi_")
        resp = {"/ot/api/171/documentos/cadena": CADENA, "/ot/api/171/documentos/logistica": RESUMEN,
                "/ot/api/171/documentos/logistica?check=1": CON_CHECK, "/ot/api/171/documentos/logistica?guia=33194": DETALLE,
                "/ot/api/171/documentos/logistica?guia=33193": dict(DETALLE, detalle=dict(DETALLE["detalle"], guia="33193", diferencias=[]))}
        est = RAIZ / "static"
        html = PAGINA % {"iconos": (est / "bootstrap-icons.css").as_uri(),     # los íconos del proyecto (local, sin red)
                         "css1": (est / "ot_doc_cadena.css").as_uri(), "css2": (est / "ot_guia_logistica.css").as_uri(),
                         "js1": (est / "ot_doc_cadena.js").as_uri(), "js2": (est / "ot_guia_logistica.js").as_uri(),
                         "resp": json.dumps(resp, ensure_ascii=False)}
        cls.ruta = pathlib.Path(cls.dir) / "trazabilidad.html"
        cls.ruta.write_text(html, encoding="utf-8")
        cls.capturas = os.environ.get("ILUS_CAPTURAS_DIR") or cls.dir

    def _foto(self, pag, nombre):
        pag.screenshot(path=os.path.join(self.capturas, f"logistica_v2_{nombre}.png"), full_page=True)

    def _revisar(self, ancho, alto, nombre):
        errores = []
        with sync_playwright() as p:
            nav = p.chromium.launch()
            pag = nav.new_page(viewport={"width": ancho, "height": alto})
            pag.on("console", lambda m: errores.append(m.text) if m.type == "error" else None)
            pag.on("pageerror", lambda e: errores.append(str(e)))
            pag.goto(self.ruta.as_uri())
            pag.wait_for_selector("[data-gl-ver='33195'] .gl-dot.verde", timeout=10000)
            puntos = pag.eval_on_selector_all("[data-gl-dot]", "els => els.map(e => e.className)")
            etiquetas = pag.eval_on_selector_all("[data-gl-ver]", "els => els.map(e => e.getAttribute('aria-label'))")
            chip = pag.inner_text("[data-gl-chip]")
            resumen = pag.inner_text("[data-gl-resumen]")
            fondo_guia = pag.eval_on_selector_all(".dc-doc.t-guia", "els => els.map(e => getComputedStyle(e).backgroundColor)")
            cifras = pag.eval_on_selector_all("[data-gl-cif]", "els => els.map(e => e.hidden ? '' : e.innerText.replace(/\\s+/g, ' ').trim())")
            ambar_cif = pag.eval_on_selector_all("[data-gl-cif] .dc-cif-c.ambar", "els => els.length")
            compacta = pag.evaluate(MEDIDAS)
            self._foto(pag, f"{nombre}_compacta")
            resumen_oculto = pag.eval_on_selector("[data-gl-resumen]", "e => e.hidden && e.innerText.trim() === ''")
            # la guía con la diferencia de Check: la VENTANA (no el panel en pantalla) con veredicto, comparación y línea de tiempo
            pag.click("[data-gl-ver='33194']")
            pag.wait_for_selector(".dc-mo .gl-tl", timeout=10000)
            en_pantalla = pag.eval_on_selector_all(".dc .gl-det, .dc .dc-panel:not([hidden])", "els => els.length")
            detalle = pag.inner_text(".dc-mo")
            veredicto = pag.inner_text(".dc-mo .gl-ver")
            orden = pag.eval_on_selector_all(".dc-mo .gl-tl-t", "els => els.map(e => e.innerText)")
            modal = pag.evaluate(MEDIDAS.replace(".dc *", ".dc-mo *"))
            caja = pag.eval_on_selector(".dc-mo-c", "e => { const r = e.getBoundingClientRect(); return [Math.round(r.width), Math.round(r.height)]; }")
            self._foto(pag, f"{nombre}_ventana_guia")
            # otra guía desde la propia ventana
            pag.click(".dc-mo [data-gl-modal-guia='33193']")
            pag.wait_for_function("document.querySelector('.dc-mo .gl-cab').innerText.includes('33193')", timeout=10000)
            pag.keyboard.press("Escape")
            cerrada = pag.eval_on_selector_all(".dc-mo", "els => els.length === 0")
            # la factura: su detalle de siempre (saldo por línea y otras OT), ahora también en la ventana
            pag.click("[data-dc-doc='0:1']")
            pag.wait_for_selector(".dc-mo .dc-sal", timeout=10000)
            factura = pag.inner_text(".dc-mo")
            self._foto(pag, f"{nombre}_ventana_factura")
            pag.click(".dc-mo-x")
            sin_ventana = pag.eval_on_selector_all(".dc-mo", "els => els.length === 0")
            nav.close()
        self.assertEqual(errores, [], errores)
        for m in (compacta, modal):
            self.assertLessEqual(m["scroll"], m["ancho"], "sin scroll horizontal")
            self.assertEqual(m["cortados"], [], "nada truncado (REGLA #15)")
            self.assertEqual(m["fuera"], [], "nada se sale de la pantalla")
        self.assertEqual(compacta["dot"], [10, 10], "semáforo chico")
        self.assertEqual(puntos, ["gl-dot ambar", "gl-dot ambar", "gl-dot verde", "gl-dot gris"])
        self.assertIn("Semáforo ámbar: La guía cuadra con la OT · bodega expidió 1 de 2", etiquetas[1])
        self.assertIn("No cuadra con la OT · 2 diferencias", chip, "1 de la guía + 1 de Check")
        self.assertTrue(resumen_oculto, "ya no hay texto bajo la fila: la trazabilidad es chiquita")
        self.assertEqual(resumen.strip(), "")
        self.assertTrue(all(f in ("rgb(255, 255, 255)", "rgba(0, 0, 0, 0)") for f in fondo_guia), fondo_guia)
        self.assertEqual([c.upper() for c in cifras], ["2 OT 1 GUÍA 1 CHECK", "2 OT 2 GUÍA 1 CHECK", "1 OT 1 GUÍA 1 CHECK", "1 OT 1 GUÍA — CHECK"],
                         "cada tarjeta de guía dice cuánto pedía la OT, cuánto sacó la guía y cuánto expidió Check")
        self.assertEqual(ambar_cif, 2, "ámbar solo donde no cuadra: guía ≠ OT (33193) y Check < guía (33194)")
        self.assertEqual(en_pantalla, 0,"el detalle NO se pinta en la pantalla: va en la ventana")
        self.assertIn("No cuadra con la OT", veredicto)
        self.assertIn(B_TEXTO, veredicto, "el motivo, con nombre completo, dentro del veredicto")
        self.assertIn("La OT gestiona 3 unidades", veredicto)
        self.assertLess(detalle.index("No cuadra con la OT"), detalle.index("COMPARACIÓN PRODUCTO POR PRODUCTO"))
        self.assertLess(detalle.index("COMPARACIÓN PRODUCTO POR PRODUCTO"), detalle.index("PASO A PASO"))
        self.assertIn(BICI, detalle)
        self.assertIn("Según Check salió 1 de 2", detalle)
        self.assertIn("Luis Alberto Pérez Contreras", detalle)
        self.assertIn("Samuel Rojas", detalle)
        self.assertEqual([o.split(" ")[0:3] for o in orden],
                         [["Se", "emitió", "nota"], ["Se", "emitió", "factura"], ["Se", "despachó", "la"],
                          ["Bodega", "juntó", "los"], ["Bodega", "controló", "y"]],
                         "línea de tiempo en orden: nota de venta → factura → guía → picking → control de salida")
        if nombre == "375":
            self.assertEqual(caja, [375, 812], "en el celular la ventana ocupa toda la pantalla")
        self.assertTrue(cerrada and sin_ventana, "Esc y la X cierran la ventana")
        self.assertIn("libre $709.539 de $961.640", factura)
        self.assertIn("OT-2026-00249", factura)
        return compacta

    def test_escritorio_1280(self):
        m = self._revisar(1280, 900, "1280")
        self.assertLessEqual(m["alto_tarjeta"], 520, "la trazabilidad cerrada: tarjetas medianas, no pantallas enteras")

    def test_celular_375(self):
        m = self._revisar(375, 812, "375")
        self.assertTrue(all(h >= 44 for h in m["chips"]), "toques de 44 px en el celular (REGLA #3): %s" % m["chips"])


if __name__ == "__main__":
    unittest.main()

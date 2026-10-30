"""«Costos y documentación de la OT» v6 en un navegador de verdad (Chromium por Playwright), 29-oct-2026.

Daniel, viendo la v5: «Me gustaba cómo salía antes, me había acostumbrado: cuánto cobré, cuánto nos cobraron y el margen en verde. Siento que lo nuevo
ocupa mucho espacio… más colorido: antes estaba un poco más espectacular. La cuenta está perfecta. El costo está perfecto.»

Página estática con datos de MENTIRA (nunca producción): el motor (static/ot_fin_motor.js + .css) con /recorrido y /panorama simulados. Se mira en
escritorio (1280, bloque de 1000 px como en la ficha) y celular (375):
  · sin errores de consola, sin scroll horizontal y nada cortado (REGLA #15);
  · «¿Quién se adjudica el dinero?» es una franja FINA: una o dos líneas en escritorio;
  · el GANAMOS de «La cuenta» es el protagonista: lleno de verde (#15803d) en la tolerancia y con la cifra más grande del bloque;
  · en el celular, «Ver el desglose» mide 44 px (REGLA #3) y las tres cifras se apilan sin salirse.
Si ILUS_CAPTURAS_DIR está definida, deja ahí las capturas. Correr con:  python -m unittest tests.test_ot_motor_navegador
"""
import json
import os
import pathlib
import tempfile
import unittest

from tests.test_ot_motor_quien_y_botones import _ejemplo_de_daniel

RAIZ = pathlib.Path(__file__).resolve().parent.parent

try:
    from playwright.sync_api import sync_playwright
except Exception:          # pragma: no cover - sin Playwright en esta máquina
    sync_playwright = None

PAGINA = """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Costos (datos de mentira)</title>
<link rel="stylesheet" href="%(icons)s"><link rel="stylesheet" href="%(css)s">
<style>body{margin:0;padding:16px;font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:#f3f4f6;color:#111827}
#otdCardMotor{max-width:1000px;margin:0 auto}</style>
<script>
const RESP = %(resp)s;
window.fetch = async (u) => { const r = RESP[u]; return { ok: !!r, status: r ? 200 : 404, json: async () => r }; };
window.ilusToast = function () {};
</script>
<script src="%(js)s" defer></script>
</head><body><div id="otdCardMotor"><div class="fm" data-fin-motor data-vid="461" data-modo="ficha"><div class="fm-cargando">Cargando…</div></div></div></body></html>"""


@unittest.skipUnless(sync_playwright, "Playwright no está instalado")
class TestMotorEnElNavegador(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="motor_nav_")
        cls.capturas = os.environ.get("ILUS_CAPTURAS_DIR") or cls.dir

    def _pagina(self, rec, pan, nombre):
        est = RAIZ / "static"
        html = PAGINA % {"icons": (est / "bootstrap-icons.css").as_uri(), "css": (est / "ot_fin_motor.css").as_uri(),
                         "js": (est / "ot_fin_motor.js").as_uri(),
                         "resp": json.dumps({"/ot/api/461/recorrido": rec, "/ot/api/461/panorama": pan}, ensure_ascii=False)}
        ruta = pathlib.Path(self.dir) / (nombre + ".html")
        ruta.write_text(html, encoding="utf-8")
        return ruta

    def _medir(self, ruta, ancho, alto, nombre):
        errores = []
        with sync_playwright() as p:
            nav = p.chromium.launch()
            pag = nav.new_page(viewport={"width": ancho, "height": alto})
            pag.on("console", lambda m: errores.append(m.text) if m.type == "error" else None)
            pag.on("pageerror", lambda e: errores.append(str(e)))
            pag.goto(ruta.as_uri())
            pag.wait_for_selector(".fm-res", timeout=10000)
            med = pag.evaluate("""() => {
              const d = document.documentElement, cortados = [];
              document.querySelectorAll('.fm *').forEach(el => {
                const cs = getComputedStyle(el);
                if (cs.textOverflow === 'ellipsis' || (cs.webkitLineClamp && cs.webkitLineClamp !== 'none')) cortados.push(el.className + ':clamp');
                if (el.children.length === 0 && el.innerText && el.offsetParent && el.scrollWidth > el.clientWidth + 1 && cs.overflowX !== 'visible')
                  cortados.push((el.className || el.tagName) + ':' + el.scrollWidth + '>' + el.clientWidth);
              });
              // cuántas líneas ocupa la franja: los ítems de una misma línea se solapan en vertical
              const lin = document.querySelector('.fm-adj-l');
              const cajas = [...lin.children].map(c => c.getBoundingClientRect()).sort((a, b) => a.top - b.top);
              let lineas = 0, fondo = -1;
              cajas.forEach(r => { if (r.top >= fondo - 1) { lineas++; fondo = r.bottom; } else fondo = Math.max(fondo, r.bottom); });
              const q = document.querySelector('.fm-res-i.q'), qb = q.querySelector('b');
              const fuera = [...document.querySelectorAll('.fm-res-i')].filter(e => e.getBoundingClientRect().right > d.clientWidth + 1).length;
              const tams = [...document.querySelectorAll('.fm b, .fm strong')].map(e => parseFloat(getComputedStyle(e).fontSize));
              return { scroll: d.scrollWidth, ancho: innerWidth, cortados, fuera,
                       lineas,
                       fondoQ: getComputedStyle(q).backgroundColor, tamQ: parseFloat(getComputedStyle(qb).fontSize), tamMax: Math.max(...tams),
                       ver: document.querySelector('.fm-adj-ver').getBoundingClientRect().height };
            }""")
            pag.screenshot(path=os.path.join(self.capturas, f"motor_v6_nav_{nombre}_{ancho}.png"), full_page=True)
            nav.close()
        self.assertEqual(errores, [], errores)
        self.assertLessEqual(med["scroll"], med["ancho"], "sin scroll horizontal")
        self.assertEqual(med["cortados"], [], "nada truncado (REGLA #15)")
        self.assertEqual(med["fuera"], 0, "ninguna de las tres cifras se sale del bloque")
        return med

    def test_escritorio_franja_fina_y_ganamos_protagonista(self):
        rec, pan = _ejemplo_de_daniel()
        m = self._medir(self._pagina(rec, pan, "tolerancia"), 1280, 900, "tolerancia")
        self.assertLessEqual(m["lineas"], 2, "la franja ocupa una o dos líneas en escritorio")
        self.assertEqual(m["fondoQ"], "rgb(21, 128, 61)", "el GANAMOS, lleno de verde (tolerancia de la REGLA #57)")
        self.assertEqual(m["tamQ"], m["tamMax"], "la cifra del GANAMOS es la más grande del bloque")

    def _perdida_ot220(self):
        """OT-2026-00220: cobramos $124.440, nos cobraron $170.000 (instalación $120.000 + despacho $50.000 que no se le cobró al cliente)."""
        rec, pan = _ejemplo_de_daniel()
        f = rec["fin"]
        f["me_cobraron"].update(tecnico=120000, despacho=50000, total=170000)
        f["cobre"].update(servicio=124440, despacho=0, total=124440)
        f["queda"].update(servicio=4440, despacho=-50000, total=-45560, pct=-36.6, mostrar=True)
        rec["autorizaciones"] = [{"tipo": "cerrar_con_perdida", "tipo_txt": "Cerrar con pérdida", "estado": "aprobada", "estado_txt": "Aprobada",
                                  "argumento": "El proveedor cobró el despacho aparte y el cliente ya había pagado la boleta.",
                                  "solicitado_por_nombre": "Víctor Betancur", "solicitado_at": "17/09/2026 11:47",
                                  "resuelto_por_nombre": "Daniel Aguilar", "resuelto_at": "17/09/2026 12:10"}]
        return rec, pan

    def test_la_perdida_dice_por_que_se_perdio(self):
        rec, pan = self._perdida_ot220()
        ruta = self._pagina(rec, pan, "perdida")
        with sync_playwright() as p:
            nav = p.chromium.launch()
            for ancho, alto in ((1280, 900), (375, 812)):
                pag = nav.new_page(viewport={"width": ancho, "height": alto})
                errores = []
                pag.on("pageerror", lambda e: errores.append(str(e)))
                pag.goto(ruta.as_uri())
                pag.wait_for_selector(".fm-perd", timeout=10000)
                txt = pag.inner_text(".fm-perd").replace("⁠", "")
                self.assertIn("¿Por qué perdimos?", txt)
                self.assertIn("el despacho costó $50.000 y no se le cobró al cliente (−$50.000)", txt)
                self.assertIn("A favor: la instalación o el servicio dejó +$4.440", txt)
                self.assertIn("Anexo N° 221", txt)
                self.assertIn("Motivo declarado por Víctor Betancur el 17/09/2026 11:47", txt)
                self.assertIn("El proveedor cobró el despacho aparte", txt)
                self.assertIn("autorizada por Daniel Aguilar", txt)
                self.assertLessEqual(pag.evaluate("document.documentElement.scrollWidth"), ancho, "sin scroll horizontal")
                self.assertEqual(errores, [])
                pag.screenshot(path=os.path.join(self.capturas, f"motor_perdida_{ancho}.png"), full_page=True)
                pag.close()
            nav.close()

    def test_sin_perdida_no_hay_explicacion(self):
        rec, pan = _ejemplo_de_daniel()
        ruta = self._pagina(rec, pan, "ganancia")
        with sync_playwright() as p:
            nav = p.chromium.launch()
            pag = nav.new_page(viewport={"width": 1280, "height": 900})
            pag.goto(ruta.as_uri())
            pag.wait_for_selector(".fm-res", timeout=10000)
            self.assertEqual(pag.locator(".fm-perd").count(), 0, "con ganancia no se dibuja la explicación")
            nav.close()

    def test_celular_375(self):
        rec, pan = _ejemplo_de_daniel()
        pan["cliente"] = rec["cliente"] = "Corporación de Gimnasios y Centros Deportivos Integrales del Sur de Chile SpA"
        m = self._medir(self._pagina(rec, pan, "largos"), 375, 812, "largos")
        self.assertGreaterEqual(m["ver"], 44, "«Ver el desglose» táctil de 44 px en el celular (REGLA #3)")


if __name__ == "__main__":
    unittest.main()

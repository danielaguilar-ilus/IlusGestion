"""Pantalla del técnico para pedir un repuesto desde la OT (static/ot_acciones.js y .css).

Daniel 2026-10-21: «que no ocupe mucho espacio, que sea práctico, ordenado, guiado, que tenga trayectoria, que alerte… algo parecido a
Retiros». Aquí se prueban las reglas PURAS (sin pantalla) ejecutándolas con Node y se vigilan por texto las reglas del proyecto:
REGLA #1 (nada de alert/confirm/prompt nativos), #6 (fechas en hora Chile), #19/#26 (el técnico no ve proveedor ni costo), #32 (sugerir, no imponer).
Necesita `node` (las pruebas de lógica se saltan si no está). Correr con:  py -m unittest tests.test_ot_acciones_repuestos_front
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(RAIZ, "static", "ot_acciones.js")
CSS = os.path.join(RAIZ, "static", "ot_acciones.css")
NODE = shutil.which("node")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as f:
        return f.read()


# Carga el archivo con un `window` y un `document` de mentira (no arranca nada) y ejecuta la expresión pedida.
_HARNESS = r"""
const fs = require('fs');
global.window = {};
global.document = { readyState: 'loading', addEventListener() {} };
global.localStorage = { getItem() { return null; }, setItem() {}, removeItem() {} };
eval(fs.readFileSync(process.argv[2], 'utf8'));
const P = window.OtAccPuro;
const salida = (function () { %s })();
process.stdout.write(JSON.stringify(salida === undefined ? null : salida));
"""


def correr(cuerpo):
    """Ejecuta `cuerpo` (JS que hace `return ...`) con las funciones puras en `P` y devuelve el resultado ya como Python."""
    # El guion va por archivo UTF-8 (por la línea de comandos de Windows los acentos se corrompen).
    with tempfile.TemporaryDirectory() as d:
        guion = os.path.join(d, "guion.js")
        with open(guion, "w", encoding="utf-8") as f:
            f.write(_HARNESS % cuerpo)
        r = subprocess.run([NODE, guion, JS], capture_output=True, encoding="utf-8", timeout=60)
    if r.returncode != 0:
        raise AssertionError("node falló:\n" + r.stderr[-1500:])
    return json.loads(r.stdout)


class TextoYSintaxis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer(JS)
        cls.css = _leer(CSS)

    @unittest.skipUnless(NODE, "falta node")
    def test_sintaxis_valida(self):
        r = subprocess.run([NODE, "--check", JS], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_sin_dialogos_nativos(self):
        # REGLA #1: solo ilusConfirm / ilusPrompt / ilusAlert / ilusToast (se ignoran los comentarios).
        for n, linea in enumerate(self.js.splitlines(), 1):
            codigo = re.sub(r"//.*$", "", linea)
            codigo = re.sub(r"/\*.*?\*/", "", codigo)
            self.assertIsNone(re.search(r"(?<![A-Za-z_.$])(alert|confirm|prompt)\s*\(", codigo), f"línea {n}: {linea.strip()[:90]}")
        self.assertIn("ilusConfirm", self.js)

    def test_el_tecnico_no_lee_plata_ni_proveedor(self):
        # REGLA #19/#26: ninguna lectura de proveedor, costo, N° de OC ni nota de gestión en la pantalla del técnico.
        self.assertIsNone(re.search(r"\.(proveedor\w*|costo\w*|oc_numero|nota_gestion|compra_numero_ticket)\b", self.js))
        # y los campos nuevos que sí puede ver (dueño, reloj, ETA) se leen solo si el servidor los manda
        for campo in ("atiende", "responde_antes", "compra_eta", "rechazo_motivo", "hitos"):
            self.assertIn(campo, self.js)

    def test_textos_nuevos(self):
        self.assertIn("No hay en bodega: la pide gestión", self.js)
        self.assertNotIn("Hay que pedirla</span>", self.js)          # M10: ya no se dice «Hay que pedirla»
        for t in ("Más acciones", "Reintentar fotos", "¿Cómo le dicen en tu empresa?", "Es otra distinta", "Usar la vista clásica",
                  "Avisar a gestión que sí hay una pieza que pedir", "Lo atiende", "Responde antes de"):
            self.assertIn(t, self.js, t)

    def test_nombres_alt_viaja_en_el_pedido(self):
        self.assertIn("o.nombres_alt = alt", self.js)

    def test_video_opcional_hasta_28_mb_y_foto_obligatoria(self):
        self.assertIn("28 * 1024 * 1024", self.js)
        self.assertIn("el video solo no basta", self.js)
        self.assertIn('id="oaVid"', self.js)

    def test_piolas_y_cinta_abren_el_flujo_nuevo(self):
        self.assertIn("abrir('pedir', key, { pre: a, semilla:", self.js)
        # el modal viejo sigue accesible como «vista clásica» (REGLA #4.2)
        self.assertIn("irAlClasico", self.js)
        self.assertIn("window.otdRepAbrir(mid, modo, tab)", self.js)

    def test_el_borrador_se_conserva_hasta_que_todo_suba(self):
        self.assertIn("if (!s.fotoExtraFallo) borrarBorrador(s.key);", self.js)
        self.assertNotIn("borrarBorrador(s.key);\n      try { if (window.otdRepRefrescar)", self.js.replace("\r\n", "\n"))

    def test_css_pulso_solo_en_la_primera_y_chips_tactiles(self):
        self.assertIn(".oa-primary.oa-pulso{animation:oaPulse", self.css)
        self.assertNotIn("white-space:nowrap;animation:oaPulse", self.css)
        self.assertIn("@media (pointer:coarse)", self.css)
        self.assertIn(".oa-plg", self.css)

    def test_css_textos_de_al_menos_12px_en_lo_tocado(self):
        for patron in (r"\.oa-badge\{[^}]*font:800 (\d+(?:\.\d+)?)px", r"\.oa-req\{font:800 (\d+(?:\.\d+)?)px",
                       r"\.oa-tile-sug\{[^}]*font:800 (\d+(?:\.\d+)?)px", r"\.oa-tr-w\{font:600 (\d+(?:\.\d+)?)px"):
            m = re.search(patron, self.css)
            self.assertIsNotNone(m, patron)
            self.assertGreaterEqual(float(m.group(1)), 12, patron)

    def test_botones_ambar_con_texto_oscuro(self):
        # REGLA #2: sobre ámbar claro, texto ámbar oscuro (nunca blanco).
        self.assertRegex(self.css, r"\.oa-dup\{[^}]*background:var\(--oa-amber-bg\)[^}]*color:var\(--oa-amber-tx\)")


@unittest.skipUnless(NODE, "falta node")
class ReglasPuras(unittest.TestCase):
    def test_baldosa_por_palabra_completa_o_raiz(self):
        r = correr("""
          const t = (n) => ({ n, full: n });
          return {
            apoyar: P.coincidePieza(t('Rodillo apoya piernas'), 'Tapiz para Apoyar Espalda Ilus Bio Banco Ajustable'),
            piola: P.coincidePieza(t('Piola'), 'Set de Piola V8 Booty Builder'),
            plural: P.coincidePieza(t('Polea con rodamiento'), 'Poleas de repuesto 60 mm'),
            familia: P.coincidePieza(t('Pad de asiento, respaldo o rodillo'), 'Tapiz espalda banco'),
            otra: P.coincidePieza(t('Cinta (banda)'), 'Kit Protector Metálico chico Para piola Freemotion'),
          };""")
        self.assertFalse(r["apoyar"], "«apoya» NO es «apoyar»: 4 letras sueltas ya no bastan")
        self.assertTrue(r["piola"])
        self.assertTrue(r["plural"])
        self.assertTrue(r["familia"])
        self.assertFalse(r["otra"])

    def test_raiz(self):
        r = correr("return [P.raizPalabra('Poleas'), P.raizPalabra('pines'), P.raizPalabra('apoyar'), P.raizPalabra('piola')];")
        self.assertEqual(r, ["polea", "pin", "apoyar", "piola"])

    def test_ficha_manda_la_de_la_pieza_elegida(self):
        r = correr("""
          const ok = (id) => ['A1', 'E1', 'R2'].includes(id);
          return [P.fichaParaLinea('E1', 'R2', ok), P.fichaParaLinea(undefined, 'R2', ok), P.fichaParaLinea('H', 'R2', ok),
                  P.fichaParaLinea('XX', 'R2', ok), P.fichaParaLinea('XX', undefined, ok), P.fichaParaLinea('A1', undefined, ok)];""")
        self.assertEqual(r, ["E1", "R2", "R2", "R2", None, "A1"])

    def test_nombres_alt(self):
        r = correr("""
          return [
            P.armarNombresAlt({ canonico: 'Pin selector', buscado: 'perilla', alias: 'perilla; pasador', escrito: '' }),
            P.armarNombresAlt({ canonico: '', buscado: '', alias: '', escrito: 'perilla del peso' }),
            P.armarNombresAlt({ canonico: 'Piola', buscado: 'piola', alias: ' PIOLA ', escrito: '' }),
            P.armarNombresAlt({ canonico: 'X', alias: 'a1,a2,a3,a4,a5,a6,a7,a8' }),
            P.armarNombresAlt({ canonico: 'X', buscado: 'a', alias: '' }),
            P.armarNombresAlt(),
          ];""")
        self.assertEqual(r[0], ["perilla", "pasador"])
        self.assertEqual(r[1], ["perilla del peso"])
        self.assertEqual(r[2], [])                      # lo mismo que el nombre canónico no es «otro nombre»
        self.assertEqual(len(r[3]), 6)                  # tope de 6
        self.assertEqual(r[4], [])                      # una letra sola no es un nombre
        self.assertEqual(r[5], [])

    def test_fechas_siempre_en_hora_chile(self):
        r = correr("""
          return [
            P.fechaChile('2026-10-21T18:03:00Z'),
            P.fechaChile('2026-10-21T15:03:00-03:00'),
            P.fechaChile(Date.UTC(2026, 9, 21, 18, 3)),
            P.fechaChile('21/10/2026 15:03'),
            P.fechaChile('21/10/2026 15:03', true),
            P.fechaChile('2026-10-21 15:03:00'),
            P.fechaChile('2026-07-21T18:03:00Z'),
            P.fechaChile('basura'),
            P.fechaChile(null),
            P.fechaChile('2026-10-28T03:00:00Z', true),
          ];""")
        self.assertEqual(r[0], "21/10/2026 15:03")      # octubre: horario de verano de Chile (UTC-3)
        self.assertEqual(r[1], "21/10/2026 15:03")
        self.assertEqual(r[2], "21/10/2026 15:03")
        self.assertEqual(r[3], "21/10/2026 15:03")
        self.assertEqual(r[4], "21/10/2026")
        self.assertEqual(r[5], "")                      # sin zona horaria no se adivina ni se muestra crudo
        self.assertEqual(r[6], "21/07/2026 14:03")      # julio: UTC-4
        self.assertEqual(r[7], "")
        self.assertEqual(r[8], "")
        self.assertEqual(r[9], "28/10/2026")
        for v in r:
            self.assertNotRegex(v, r"\d{4}-\d{2}-\d{2}T")   # nunca ISO crudo (REGLA #6)

    def test_hace_cuanto(self):
        r = correr("""
          const a = 1000000000000, m = 60000;
          return [P.haceTxt(a, a + 20000), P.haceTxt(a, a + 5 * m), P.haceTxt(a, a + 125 * m), P.haceTxt(a, a + 26 * 60 * m), P.haceTxt(a, a + 3 * 1440 * m), P.haceTxt(null, a)];""")
        self.assertEqual(r, ["hace un momento", "hace 5 min", "hace 2 h", "hace 1 día", "hace 3 días", ""])

    def test_aviso_de_duplicado_sugiere_y_no_bloquea(self):
        r = correr("""
          const sols = [
            { id: 1, nombre: 'Piola (cable de acero)', estado: 'solicitado', abierta: true, creada_ms: 1000000000000 },
            { id: 2, nombre: 'Polea con rodamiento', estado: 'instalado', abierta: false },
          ];
          return {
            igual: (P.buscarDuplicado('Piola', sols) || {}).id,
            otra: P.buscarDuplicado('Pin selector', sols),
            cerrada: P.buscarDuplicado('Polea con rodamiento', sols),
            vacio: P.buscarDuplicado('Piola', []),
            texto: P.dupTexto(sols[0], 1000000000000 + 5 * 60000),
          };""")
        self.assertEqual(r["igual"], 1)
        self.assertIsNone(r["otra"])
        self.assertIsNone(r["cerrada"])                 # un pedido ya instalado/cerrado no cuenta como duplicado
        self.assertIsNone(r["vacio"])
        self.assertEqual(r["texto"], "Ya hay un pedido de Piola para este equipo (hace 5 min, en revisión).")

    def test_trayectoria(self):
        r = correr("""
          const f = (x) => P.trayectoria(x).pasos.map(p => p.estado[0] + ':' + p.texto + (p.extra ? '|' + p.extra : '') + (p.cuando ? '@' + p.cuando : ''));
          return {
            nuevo: f({ estado: 'solicitado', creada_ms: Date.UTC(2026, 9, 21, 18, 3) }),
            revisado: f({ estado: 'validado' }),
            pedido: f({ estado: 'pedido', compra_eta: '28/10/2026', hitos: { solicitado: '21/10/2026 15:03', validado: '21/10/2026 16:00', pedido: '22/10/2026 09:10' } }),
            apartado: f({ estado: 'validado', apartado: true }),
            lista: f({ estado: 'recibido' }),
            instalada: f({ estado: 'instalado' }),
            rechazado: P.trayectoria({ estado: 'rechazado', rechazo_motivo: 'Ya había una pieza igual en bodega.' }),
          };""")
        self.assertEqual(r["nuevo"], ["h:Solicitado@21/10/2026 15:03", "a:Revisado por bodega", "p:Pedido o apartado", "p:Lista en bodega", "p:Instalada"])
        self.assertEqual(r["revisado"][0:3], ["h:Solicitado", "h:Revisado por bodega", "a:Pedido o apartado"])
        self.assertEqual(r["pedido"], ["h:Solicitado@21/10/2026 15:03", "h:Revisado por bodega@21/10/2026 16:00",
                                       "h:Pedido|llega ~28/10/2026@22/10/2026 09:10", "a:Lista en bodega", "p:Instalada"])
        self.assertEqual(r["apartado"][2], "h:Apartado")
        self.assertEqual(r["apartado"][3], "a:Lista en bodega")
        self.assertEqual([p[0] for p in r["lista"]], ["h", "h", "h", "h", "a"])
        self.assertEqual([p[0] for p in r["instalada"]], ["h"] * 5)
        self.assertTrue(r["rechazado"]["rechazado"])
        self.assertEqual(r["rechazado"]["motivo"], "Ya había una pieza igual en bodega.")
        self.assertEqual(r["rechazado"]["pasos"], [])

    def test_sin_datos_del_servidor_no_se_inventa_nada(self):
        r = correr("""
          const t = P.trayectoria({ estado: 'solicitado' });
          return t.pasos.map(p => p.cuando + '/' + p.extra);""")
        self.assertEqual(r, ["/", "/", "/", "/", "/"])

    def test_video(self):
        r = correr("return [P.esVideoArchivo({ type: 'video/mp4', name: 'a' }), P.esVideoArchivo({ type: '', name: 'clip.MOV' }), P.esVideoArchivo({ type: 'image/jpeg', name: 'a.jpg' }), P.esVideoArchivo(null)];")
        self.assertEqual(r, [True, True, False, False])


if __name__ == "__main__":
    unittest.main()

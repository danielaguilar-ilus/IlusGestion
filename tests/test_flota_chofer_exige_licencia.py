# -*- coding: utf-8 -*-
"""🚚 FLOTA · Sin licencia subida NO se crea un chofer (Daniel, 05-nov-2026):
«que quede estricto que tienen que subir la licencia para poder avanzar a la creación del chofer. Si no, está prohibido crear choferes sin licencia».

Decisiones que se vigilan:
  1. SERVIDOR: sin archivo de licencia no se crea (400) para NADIE, tampoco el superadministrador: «Registrar sin comprobar» omite la
     COMPROBACIÓN, no el archivo. Un archivo ilegible o que no es una licencia (PDF/foto) tampoco crea nada.
  2. PANTALLA: «Guardar chofer» queda cerrado mientras falte la licencia o su fecha, con el aviso a la vista; guardar por la tecla Enter
     también se niega.
"""
import json
import os
import re
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_licencia_escaner import ADMIN, SUPER, BaseEscaner, texto_licencia  # noqa: E402
from test_flota_solicitudes import _datos_chofer, _leer  # noqa: E402

JS = _leer("static", "flota_uso.js")
HTML = _leer("templates", "flota", "index.html")
CSS = _leer("static", "flota.css")
MODULO = _leer("flota_module.py")
MOTIVO = "Vi la licencia en persona y el escaneo no sirvió para nada"


class TestServidorNoCreaSinLicencia(BaseEscaner):
    def cuantos(self):
        return len(self.base.cho)

    def test_sin_archivo_no_se_crea_para_nadie(self):
        antes = self.cuantos()
        for h in (SUPER, ADMIN):
            r = self.crear(headers=h, archivo=None)
            self.assertEqual(r.status_code, 400, h)
            self.assertIn("licencia_archivo", r.get_json()["errores"], h)
        self.assertEqual(self.cuantos(), antes)
        self.assertFalse(self.leidos)                                    # ni siquiera se intentó leer algo

    def test_el_superadmin_con_motivo_tampoco_puede_crear_sin_archivo(self):
        antes = self.cuantos()
        r = self.crear(headers=SUPER, archivo=None, licencia_sin_verificar_motivo=MOTIVO)
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_archivo", r.get_json()["errores"])
        self.assertEqual(self.cuantos(), antes)

    def test_un_archivo_vacio_o_que_no_es_licencia_no_crea(self):
        antes = self.cuantos()
        for archivo in ((b"", "licencia.pdf", "application/pdf"), (b"hola, soy un texto", "licencia.txt", "text/plain"),
                        (b"MZ\x90\x00 programa", "licencia.exe", "application/octet-stream")):
            r = self.crear(archivo=archivo)
            self.assertEqual(r.status_code, 400, archivo[1])
            self.assertIn("licencia_archivo", r.get_json()["errores"], archivo[1])
        self.assertEqual(self.cuantos(), antes)

    def test_una_licencia_que_el_lector_no_puede_leer_no_crea(self):
        from flota_licencia_ocr import ArchivoIlegible
        self.error_lector = ArchivoIlegible("no se pudo leer")
        antes = self.cuantos()
        for h in (SUPER, ADMIN):
            r = self.crear(headers=h)
            self.assertEqual(r.status_code, 400, h)
        self.assertEqual(self.cuantos(), antes)

    def test_una_licencia_de_otra_persona_no_crea(self):
        self.lee(texto_licencia(_datos_chofer(5)["rut"]))
        antes = self.cuantos()
        r = self.crear()
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.cuantos(), antes)

    def test_con_la_licencia_subida_y_bien_leida_si_se_crea(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.crear()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue([a for a in self.base.arch.values() if a["entidad"] == "chofer" and a["activo"]])      # y su archivo quedó guardado

    def test_el_superadmin_sin_comprobar_exige_igual_el_archivo_y_lo_guarda(self):
        self.error_lector = RuntimeError("el lector se cayó")
        r = self.crear(headers=SUPER, licencia_sin_verificar_motivo=MOTIVO)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue([a for a in self.base.arch.values() if a["entidad"] == "chofer" and a["activo"]])

    def test_quien_solo_administra_no_puede_saltarse_la_comprobacion(self):
        self.error_lector = RuntimeError("el lector se cayó")
        antes = self.cuantos()
        r = self.crear(headers=ADMIN, licencia_sin_verificar_motivo=MOTIVO)
        self.assertNotEqual(r.status_code, 200)
        self.assertEqual(self.cuantos(), antes)

    def test_la_ruta_generica_de_archivos_no_deja_subir_la_licencia_sin_pasar_por_el_escaner(self):
        cid = self.registrar_daniela()
        r = self.c.post("/flota/api/archivos", headers=SUPER, content_type="multipart/form-data", data={
            "entidad": "chofer", "entidad_id": str(cid), "tipo": "licencia", "archivo": (__import__("io").BytesIO(b"%PDF-1.4\n%%EOF"), "otra.pdf", "application/pdf")})
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (409, "LICENCIA_SOLO_POR_FORMULARIO"))

    def test_hay_un_solo_lugar_que_crea_choferes(self):
        self.assertEqual(len(re.findall(r"INSERT INTO flota_choferes", MODULO)), 1)
        i = MODULO.index("INSERT INTO flota_choferes")
        # y esa creación está dentro de la ruta que exige la licencia obligatoria
        previo = MODULO[max(0, i - 6000):i]
        self.assertIn("obligatoria=True", previo)
        self.assertIn("_licencia_exigir(", previo)


class TestPantallaPuerta(unittest.TestCase):
    def test_el_aviso_nace_visible_y_con_candado(self):
        m = re.search(r'<p class="[^"]*fl-escaner-puerta[^"]*" id="flChPuertaLic"[^>]*>', HTML)
        self.assertIsNotNone(m)
        self.assertNotIn("hidden", m.group(0))
        self.assertIn('role="status"', m.group(0))
        self.assertIn("Sin la licencia no se puede crear al chofer", HTML)

    def test_el_boton_cerrado_se_ve_cerrado(self):
        self.assertIn("#flChGuardar:disabled{opacity:.55;cursor:not-allowed}", CSS)

    def test_se_evalua_al_abrir_y_en_cada_cambio(self):
        i = JS.index("function evaluarCh()")
        self.assertIn("aplicarPuertaLicencia(r);", JS[i:i + 2400])
        j = JS.index("function abrirChofer(")
        self.assertIn("evaluarCh();", JS[j:j + 5200])

    def test_guardar_se_niega_antes_que_nada_aunque_llegue_por_enter(self):
        i = JS.index("function guardarCh()")
        t = JS[i:i + 900]
        self.assertLess(t.index("puertaLicencia(ev.reglas)"), t.index("ev.faltan.length"))
        self.assertIn("if (!puerta.abierta)", t)
        self.assertIn("fc.addEventListener('submit', function (e) { e.preventDefault(); guardarCh(); });", JS)

    def test_el_boton_no_se_reabre_solo_mientras_se_guarda(self):
        i = JS.index("function aplicarPuertaLicencia(")
        t = JS[i:i + 500]
        self.assertIn("$('flChGuardar').disabled = !p.abierta || !!S.guardandoCh;", t)

    def test_tras_un_rechazo_del_servidor_se_vuelve_a_aplicar_la_puerta(self):
        i = JS.index("if (!r.ok) {\n          S.guardandoCh = false;".replace("\n", "\r\n" if "\r\n" in JS else "\n"))
        self.assertIn("aplicarPuertaLicencia();", JS[i:i + 140])

    def test_la_puerta_decide_bien(self):
        """Ejecuta puertaLicencia() con Node."""
        i = JS.index("function puertaLicencia(r)")
        j = JS.index("function aplicarPuertaLicencia(")
        guion = JS[i:j] + r"""
function r(a, v, c) { return { licencia_archivo: { estado: a, msg: a === 'bad' ? 'Escanea o sube la licencia (foto o PDF): sin ella no se registra al chofer.' : '' },
  licencia_vence: { estado: v, msg: v === 'bad' ? 'Escanea la licencia: la fecha de vencimiento sale de la foto, no se escribe a mano.' : '' },
  licencia_clases: { estado: c || 'ok', msg: '' } }; }
var out = [];
var a = puertaLicencia(r('bad', 'bad')); out.push([a.abierta, /^Sin la licencia no se puede crear al chofer\. Escanea o sube la licencia/.test(a.texto)]);
var b = puertaLicencia(r('ok', 'bad')); out.push([b.abierta, /fecha de vencimiento sale de la foto/.test(b.texto)]);
var c = puertaLicencia(r('ok', 'ok')); out.push([c.abierta, c.texto]);
var d = puertaLicencia(r('warn', 'warn')); out.push([d.abierta, d.texto]);            // «sin comprobar» del superadmin: el archivo está, la puerta abre
var e = puertaLicencia(r('ok', 'ok', 'bad')); out.push([e.abierta, e.texto]);       // las clases se marcan después: no son la puerta
var f = puertaLicencia(r('bad', 'ok')); out.push([f.abierta]);
var g = puertaLicencia({}); out.push([g.abierta]);                                   // sin reglas no hay nada que cierre (el servidor decide)
console.log(JSON.stringify(out));
"""
        res = subprocess.run(["node", "-e", guion], capture_output=True, text=True, timeout=60)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(json.loads(res.stdout.strip()), [[False, True], [False, True], [True, ""], [True, ""], [True, ""], [False], [True]])


if __name__ == "__main__":
    unittest.main()

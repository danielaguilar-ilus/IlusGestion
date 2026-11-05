# -*- coding: utf-8 -*-
"""🚚 FLOTA · El vencimiento de la licencia NO se escribe a mano (Daniel, 05-nov-2026):
«elimina la entrada a mano para que la persona no pueda burlar los controles: que suban la imagen y ella coloque el vencimiento».

Decisiones que se vigilan:
  1. La fecha de control sale de la licencia escaneada; la persona solo ELIGE entre las fechas que se leyeron. El campo es de solo lectura.
  2. Única excepción: el superadministrador que registra «sin comprobar» (con motivo escrito y anotado): ahí no hay fecha leída.
  3. El servidor lo exige igual: cambiar RUT, clases o vencimiento sin escanear se rechaza SIEMPRE, también a un chofer antiguo que nunca
     tuvo su licencia escaneada (antes solo se rechazaba si ya tenía una).
"""
import datetime as dt
import json
import os
import re
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_licencia_escaner import ADMIN, SUPER, BaseEscaner, texto_licencia  # noqa: E402
from test_flota_solicitudes import FUTURO, _datos_chofer, _leer, _rut  # noqa: E402

JS = _leer("static", "flota_uso.js")
HTML = _leer("templates", "flota", "index.html")
CSS = _leer("static", "flota.css")
MODULO = _leer("flota_module.py")


class TestServidor(BaseEscaner):
    def sin_licencia_vigente(self):
        """Un chofer antiguo: registrado cuando la licencia todavía no era obligatoria (su archivo no está vigente)."""
        cid = self.registrar_daniela()
        for a in self.base.arch.values():
            a["activo"] = 0
        return cid

    def externo_antiguo(self):
        """Un chofer antiguo SIN usuario de ILUS (el «externo»): el único que podía corregirse sin adjuntar la licencia."""
        cid = self.sin_licencia_vigente()
        self.base.cho[cid]["usuario_id"] = None
        return cid

    def test_un_chofer_con_usuario_sin_licencia_escaneada_no_se_edita_sin_escanearla(self):
        cid = self.sin_licencia_vigente()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=200)).isoformat()
        for h in (SUPER, ADMIN):
            r = self.editar(cid, headers=h, licencia_vence=nueva)
            self.assertEqual(r.status_code, 400, h)
            self.assertIn("licencia_archivo", r.get_json()["errores"])
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(FUTURO))

    def test_un_externo_antiguo_ya_no_cambia_el_vencimiento_el_rut_ni_las_clases_a_mano(self):
        cid = self.externo_antiguo()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=200)).isoformat()
        for campo in ({"licencia_vence": nueva}, {"licencia_clases": ["B", "C"]}, {"rut": _rut(9000001)}):
            for h in (SUPER, ADMIN):
                r = self.editar(cid, headers=h, telefono="+56911112222", **campo)
                self.assertEqual((r.status_code, r.get_json().get("error_codigo")), (400, "LICENCIA_REQUIERE_ESCANEO"), (campo, h, r.get_data(as_text=True)))
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(FUTURO))

    def test_a_un_externo_antiguo_si_se_le_corrigen_otros_datos(self):
        cid = self.externo_antiguo()
        r = self.editar(cid, telefono="+56911112222", notas="Prefiere salir temprano")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["notas"], "Prefiere salir temprano")

    def test_el_superadmin_sin_comprobar_sigue_pudiendo_con_motivo(self):
        cid = self.externo_antiguo()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=30)).isoformat()
        r = self.editar(cid, telefono="+56911112222", licencia_vence=nueva, licencia_sin_verificar_motivo="Vi la licencia en persona y el escaneo no sirvió")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(nueva))
        det = [json.loads(x["detalle"] or "null") for x in self.base.clog if x["chofer_id"] == cid and x["accion"] == "editado"][-1]
        self.assertIn("licencia_sin_verificar", det)

    def test_quien_administra_sin_ser_superadmin_no_puede_saltarse_nada(self):
        cid = self.externo_antiguo()
        nueva = (dt.date.fromisoformat(FUTURO) + dt.timedelta(days=30)).isoformat()
        r = self.editar(cid, headers=ADMIN, telefono="+56911112222", licencia_vence=nueva,
                        licencia_sin_verificar_motivo="Vi la licencia en persona y el escaneo no sirvió")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.base.cho[cid]["licencia_vence"], dt.date.fromisoformat(FUTURO))

    def test_al_registrar_la_fecha_tiene_que_estar_en_la_foto(self):
        self.lee(texto_licencia(_datos_chofer(4)["rut"]))
        r = self.crear(licencia_vence=(dt.date.fromisoformat(FUTURO) + dt.timedelta(days=500)).isoformat())
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_vence", r.get_json()["errores"])


class TestPantalla(unittest.TestCase):
    def test_el_campo_nace_de_solo_lectura_y_dice_por_que(self):
        m = re.search(r'<input type="date" id="flChVence"[^>]*>', HTML)
        self.assertIsNotNone(m)
        for atributo in ("readonly", "disabled", 'name="licencia_vence"'):
            self.assertIn(atributo, m.group(0))
        self.assertIn("No se escribe a mano", HTML)
        self.assertIn('id="flChVenceAyuda"', HTML)

    def test_se_ve_bloqueado_tambien_en_el_celular(self):
        # un campo deshabilitado se ve desteñido: se fuerza el color para que la fecha se lea completa
        self.assertRegex(CSS, r"#flChVence:disabled\{[^}]*-webkit-text-fill-color:currentColor")

    def test_la_fecha_solo_se_escribe_desde_tres_lugares(self):
        # abrir el formulario (la ya guardada), aplicar la lectura del escáner y elegir una de las fechas leídas. Nada más.
        escrituras = re.findall(r"elements\.licencia_vence\.value\s*=", JS)
        self.assertEqual(len(escrituras), 3, escrituras)
        i = JS.index("function aplicarLecturaCh(")
        self.assertIn("f.elements.licencia_vence.value = l.vence", JS[i:i + 600])
        self.assertIn("elements.licencia_vence.value = fb.getAttribute('data-lec-fecha')", JS)

    def test_nadie_escucha_lo_que_se_teclea_en_el_vencimiento(self):
        self.assertNotRegex(JS, r"flChVence['\"]\)\.addEventListener")
        self.assertNotRegex(JS, r"elements\.licencia_vence\.(readOnly|disabled)\s*=\s*(b|false)\b")

    def test_leer_o_cambiar_la_omision_vuelve_a_bloquear_o_liberar_el_campo(self):
        i = JS.index("function pintarLectura()")
        self.assertIn("bloquearVence();", JS[i:i + 200])
        j = JS.index("function aplicarBloqueoCh()")
        self.assertIn("bloquearVence();", JS[j:j + 400])

    def test_sin_escanear_el_mensaje_manda_a_escanear(self):
        self.assertIn("Escanea la licencia: la fecha de vencimiento sale de la foto, no se escribe a mano.", JS)

    def test_el_campo_se_bloquea_o_libera_de_verdad(self):
        """Ejecuta bloquearVence() con Node y un campo de mentira."""
        i = JS.index("function bloquearVence()")
        j = JS.index("function aplicarBloqueoCh()")
        fn = JS[i:j]
        guion = fn + r"""
var campo = { readOnly: null, disabled: null }, ayuda = { textContent: '' }, S = { chOmitir: '' };
function $(id) { return id === 'flFormCh' ? { elements: { licencia_vence: campo } } : ayuda; }
var out = [];
bloquearVence(); out.push([campo.readOnly, campo.disabled, /No se escribe a mano/.test(ayuda.textContent)]);
S.chOmitir = 'Vi la licencia en persona y el escaneo no sirvió';
bloquearVence(); out.push([campo.readOnly, campo.disabled, /sin comprobar/i.test(ayuda.textContent)]);
S.chOmitir = '';
bloquearVence(); out.push([campo.readOnly, campo.disabled, /No se escribe a mano/.test(ayuda.textContent)]);
console.log(JSON.stringify(out));
"""
        r = subprocess.run(["node", "-e", guion], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout.strip()), [[True, True, True], [False, False, True], [True, True, True]])

    def test_el_servidor_no_depende_de_que_ya_tuviera_licencia(self):
        i = MODULO.index('if _lic_verificar() and data_lic is None and')
        linea = MODULO[i:i + 160]
        self.assertNotIn("tiene_lic", linea)
        self.assertIn('"licencia_vence"', linea)


if __name__ == "__main__":
    unittest.main()

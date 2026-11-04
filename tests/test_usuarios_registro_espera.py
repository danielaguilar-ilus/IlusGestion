# -*- coding: utf-8 -*-
"""Usuario nuevo «en espera» y su registro (Daniel, 2026-11-04):
«para crear un usuario nuevo solo necesito su correo y su cargo; él debe rellenar su RUT, nombre y apellido, su dirección y su teléfono
con formato chileno · estará stand by si no se registra».
Cubre: reglas puras (usuarios_registro.py), el cableado del servidor (alta rápida, registro en /welcome, reenvío de invitación) y las
tres plantillas (formulario de alta, bienvenida y lista de usuarios)."""
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import usuarios_registro as ur  # noqa: E402
from tests._fuente_app import RAIZ, fuente_funcion  # noqa: E402


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


# ══════════════════════════════════ A) reglas puras ══════════════════════════════════
class TestNombre(unittest.TestCase):
    def test_nombres_validos(self):
        for ok in ("Ana", "María José", "O'Higgins", "Pérez-Soto", "Ñuñoa", "Li"):
            self.assertTrue(ur.validar_parte_nombre(ok)[0], ok)

    def test_nombres_invalidos(self):
        for mal in ("", "   ", "A", "Ana3", "Ana_Soto", "<b>Ana</b>", "Ana; DROP", "x" * 70, "-Ana", "Ana-"):
            self.assertFalse(ur.validar_parte_nombre(mal)[0], repr(mal))

    def test_el_mensaje_nombra_el_campo(self):
        self.assertEqual(ur.validar_parte_nombre("", "Tu apellido"), (False, "Tu apellido es obligatorio."))
        self.assertIn("Tu nombre", ur.validar_parte_nombre("Ana3", "Tu nombre")[1])

    def test_todo_en_minuscula_o_mayuscula_se_ordena(self):
        self.assertEqual(ur.validar_parte_nombre("maría josé"), (True, "María José"))
        self.assertEqual(ur.validar_parte_nombre("GONZÁLEZ  PÉREZ"), (True, "González Pérez"))
        self.assertEqual(ur.validar_parte_nombre("McDonald"), (True, "McDonald"), "lo escrito con mayúsculas mixtas se respeta")

    def test_armar_y_separar(self):
        self.assertEqual(ur.armar_nombre("ana  maría", "soto pérez"), "Ana María Soto Pérez")
        self.assertEqual(ur.separar_nombre("Ana María Soto Pérez"), ("Ana", "María Soto Pérez"))
        self.assertEqual(ur.separar_nombre("Ana"), ("Ana", ""))
        self.assertEqual(ur.separar_nombre(""), ("", ""))
        self.assertEqual(ur.separar_nombre(None), ("", ""))


class TestTelefono(unittest.TestCase):
    def test_formatos_chilenos_validos(self):
        for raw in ("9 1234 5678", "912345678", "+56 9 1234 5678", "56912345678", "+56912345678", "2 2345 6789", "322123456", "9-1234-5678"):
            ok, v = ur.telefono_cl(raw)
            self.assertTrue(ok, raw)
            self.assertRegex(v, r"^\+56[2-9][0-9]{8}$")

    def test_normaliza_a_mas56(self):
        self.assertEqual(ur.telefono_cl("9 1234 5678"), (True, "+56912345678"))
        self.assertEqual(ur.telefono_cl("56 2 2345 6789"), (True, "+56223456789"))

    def test_invalidos(self):
        for raw in ("", "   ", "12345", "1234567890", "012345678", "112345678", "9123456", "abc", "+1 555 123 4567", "9 1234 56789"):
            ok, msg = ur.telefono_cl(raw)
            self.assertFalse(ok, repr(raw))
            self.assertEqual(msg, ur.MSG_TEL)

    def test_para_formulario(self):
        self.assertEqual(ur.telefono_para_formulario("+56912345678"), "9 1234 5678")
        self.assertEqual(ur.telefono_para_formulario("+56223456789"), "2 2345 6789")
        self.assertEqual(ur.telefono_para_formulario(None), "")
        self.assertEqual(ur.telefono_para_formulario("123"), "123")


class TestProvisionalYEspera(unittest.TestCase):
    def test_nombre_provisional(self):
        self.assertEqual(ur.nombre_provisional("juan.perez@sphs.cl"), "Juan Perez")
        self.assertEqual(ur.nombre_provisional("maria_gonzalez99@gmail.com"), "Maria Gonzalez")
        self.assertEqual(ur.nombre_provisional("123@x.cl"), "Usuario nuevo")
        self.assertEqual(ur.nombre_provisional(""), "Usuario nuevo")
        self.assertLessEqual(len(ur.nombre_provisional("a" * 200 + "@x.cl")), 60)

    def test_nombre_es_provisional(self):
        self.assertTrue(ur.nombre_es_provisional("Juan Perez", "juan.perez@sphs.cl"))
        self.assertTrue(ur.nombre_es_provisional("", "juan.perez@sphs.cl"))
        self.assertTrue(ur.nombre_es_provisional(None, "x@y.cl"))
        self.assertFalse(ur.nombre_es_provisional("Juan Pérez Soto", "juan.perez@sphs.cl"))

    def test_en_espera_es_inactivo_sin_clave_propia_y_sin_haber_entrado(self):
        self.assertTrue(ur.esta_en_espera({"active": 0, "requires_password_change": 1, "last_login_at": None}))
        self.assertTrue(ur.esta_en_espera({"active": False, "requires_password_change": True}))
        self.assertTrue(ur.esta_en_espera({"active": "0", "requires_password_change": "1"}))

    def test_no_esta_en_espera(self):
        self.assertFalse(ur.esta_en_espera(None))
        self.assertFalse(ur.esta_en_espera({}))
        self.assertFalse(ur.esta_en_espera({"active": 1, "requires_password_change": 1}), "activo: ya entró o lo activaron")
        self.assertFalse(ur.esta_en_espera({"active": 0, "requires_password_change": 0}), "desactivado a propósito, ya tenía clave")
        self.assertFalse(ur.esta_en_espera({"active": 0, "requires_password_change": 1, "last_login_at": "2026-10-01"}),
                         "ya inició sesión alguna vez: está desactivado, no en espera")
        self.assertFalse(ur.esta_en_espera({"active": None, "requires_password_change": 1}))


# ══════════════════════════════════ B) el servidor ══════════════════════════════════
class TestServidor(unittest.TestCase):
    def test_alta_rapida_por_defecto_solo_correo_y_cargo(self):
        f = fuente_funcion("new_user")
        self.assertIn('rapido = request.form.get("modo", "invitar") != "completo"', f)
        rapido = f.split("if rapido:")[1].split("errors = []")[0]
        self.assertIn("nombre = _uregi.nombre_provisional(username)", rapido)
        self.assertIn("active, send_invite = 0, True", rapido)
        self.assertIn('phone, wa_number, manual_password = "", "", ""', rapido, "sin teléfono ni clave manual: la clave la crea la propia persona")

    def test_en_modo_rapido_no_se_leen_ni_se_validan_los_datos_personales(self):
        f = fuente_funcion("new_user")
        rapido, completo = f.split("        if rapido:\n            # Invitación rápida")[1].split("        else:\n")[:2]
        self.assertIn("rut_val, fecha_nac_val = None, None", rapido)
        self.assertIn('"accion": "limpiar"', rapido)
        self.assertNotIn("request.form", rapido)
        # el formulario completo de siempre sigue validando todo (REGLA #4.2: no se quitó nada)
        for pieza in ("_validar_rut_usuario(", "_uperf.validar_fecha_nacimiento(", "_uperf.resolver_direccion("):
            self.assertIn(pieza, completo)

    def test_el_cargo_sigue_siendo_el_rol_validado(self):
        f = fuente_funcion("new_user")
        self.assertIn("_es_rol_valido(role)", f)
        self.assertIn('role == "superadmin" and not is_superadmin', f)

    def test_el_usuario_rapido_nace_sin_clave_propia(self):
        f = fuente_funcion("new_user")
        self.assertIn("needs_change = 0 if manual_password else 1", f)
        self.assertIn("secrets.token_hex(32)", f)

    def test_registro_valida_todo_y_activa_solo_al_que_estaba_en_espera(self):
        f = fuente_funcion("welcome_activate")
        for pieza in ('_uregi.validar_parte_nombre(nombre_in, "Tu nombre")', '_uregi.validar_parte_nombre(apellido_in, "Tu apellido")',
                      "_uregi.telefono_cl(phone_in)", '_validar_rut_usuario(rut_raw, excluir_id=row["user_id"])',
                      "_uperf.resolver_direccion(direccion, lat_in, lng_in, pid_in)", '"Tu RUT es obligatorio."',
                      'dir_val["accion"] != "guardar"', "_password_strength_errors(pw1)"):
            self.assertIn(pieza, f)
        self.assertRegex(f, r"if _en_espera:\n\s+cur\.execute\(f\"UPDATE `\{AUTH_TABLE\}` SET active=1 WHERE id=%s\"")
        self.assertEqual(f.count("SET active=1"), 1)

    def test_el_registro_no_pisa_la_fecha_de_nacimiento_que_ya_tuviera(self):
        f = fuente_funcion("welcome_activate")
        self.assertIn('_guardar_perfil_usuario(cur, "id", row["user_id"], rut_val, _ufila.get("fecha_nac"), dir_val, comuna, ciudad)', f)

    def test_si_algo_falla_no_se_guarda_nada_y_se_conserva_lo_escrito(self):
        f = fuente_funcion("welcome_activate")
        antes_de_guardar, _ = f.split("new_hash = generate_password_hash(pw1)")
        self.assertIn("return _form_welcome(**eco)", antes_de_guardar)
        self.assertNotIn("UPDATE", antes_de_guardar.split("if problemas:")[0].split('if request.method == "POST":')[1])

    def test_el_enlace_sigue_siendo_de_un_solo_uso_y_con_limite_de_intentos(self):
        f = fuente_funcion("welcome_activate", con_decoradores=True)
        self.assertIn('@rate_limited("welcome"', f)
        self.assertIn("r.used=0 AND r.expires_at > UTC_TIMESTAMP()", f)
        self.assertIn("SET used=1", f)

    def test_reenviar_a_un_usuario_en_espera_manda_el_enlace_de_registro(self):
        f = fuente_funcion("invite_user")
        self.assertIn("_uregi.esta_en_espera(_fila_espera)", f)
        self.assertIn('url_for("welcome_activate", token=token, _external=True)', f)
        self.assertIn("minutes=10080", f)
        self.assertIn('url_for("reset_password", token=token, _external=True)', f, "el resto de los usuarios sigue con el enlace de siempre")

    def test_no_se_agrega_ninguna_columna_a_app_users(self):
        """REGLA #18: «en espera» se deriva de columnas que ya existen; el arranque no toca app_users por esto."""
        src = open(os.path.join(RAIZ, "usuarios_registro.py"), encoding="utf-8").read()
        self.assertNotIn("ALTER TABLE", src)
        for n in ("new_user", "welcome_activate", "invite_user"):
            self.assertNotIn("ALTER TABLE", fuente_funcion(n))


# ══════════════════════════════════ C) las plantillas ══════════════════════════════════
class TestPlantillas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import jinja2
        except ImportError:
            raise unittest.SkipTest("jinja2 no está instalado")
        base = ("{% block title %}{% endblock %}{% block head_extra %}{% endblock %}"
                "{% block content %}{% endblock %}{% block scripts %}{% endblock %}")
        cls.env = jinja2.Environment(
            loader=jinja2.DictLoader({"base.html": base, "user_form.html": _leer("templates", "user_form.html"),
                                      "welcome.html": _leer("templates", "welcome.html")}),
            autoescape=True)
        cls.env.filters["rut_fmt"] = lambda v: v or ""
        cls.env.globals.update(
            csrf_token=lambda: "tok", url_for=lambda ep, **kw: "/" + ep + (("/" + kw["filename"]) if "filename" in kw else ""),
            permissions={"superadmin": True}, get_flashed_messages=lambda **kw: [])

    def _welcome(self, **ctx):
        base = dict(token="t", nombre="", email="ana@sphs.cl", marca={"name": "ILUS Fitness"}, en_espera=True, nombre_in="", apellido_in="",
                    phone_in="", rut="", direccion="", comuna="", ciudad="", direccion_lat="", direccion_lng="", direccion_place_id="",
                    google_maps_api_key="")
        base.update(ctx)
        return self.env.get_template("welcome.html").render(**base)

    def _form(self, **ctx):
        base = dict(errors=[], user=None, fd={}, roles=[])
        base.update(ctx)
        return self.env.get_template("user_form.html").render(**base)

    # —— bienvenida ——
    def test_bienvenida_pide_todo_antes_de_la_clave_y_sin_campos_repetidos(self):
        html = self._welcome()
        orden = [html.index(f'name="{n}"') for n in ("nombre", "apellido", "rut", "phone", "direccion", "password")]
        self.assertEqual(orden, sorted(orden), "nombre, apellido, RUT, teléfono y dirección van antes de la contraseña")
        for n in ("nombre", "apellido", "rut", "phone", "direccion", "comuna", "ciudad", "password", "password2",
                  "direccion_lat", "direccion_lng", "direccion_place_id"):
            self.assertEqual(len(re.findall(rf'<input[^>]*name="{n}"', html)), 1, f"el campo {n} aparece una sola vez")

    def test_bienvenida_marca_los_campos_como_obligatorios_y_usa_formato_chileno(self):
        html = self._welcome()
        for i in ("wNombre", "wApellido", "wRut", "wPhone", "wDireccion"):
            self.assertRegex(html, rf'<input[^>]*id="{i}"[^>]*required')
        self.assertIn('placeholder="12.345.678-5"', html)
        self.assertIn('placeholder="9 1234 5678"', html)
        self.assertIn(">+56</span>", html)
        for readonly in ("wComuna", "wCiudad"):
            self.assertRegex(html, rf'<input[^>]*id="{readonly}"[^>]*readonly')

    def test_bienvenida_avisa_que_esta_en_espera_y_saluda_sin_nombre_provisional(self):
        self.assertIn("en espera", self._welcome(en_espera=True))
        self.assertNotIn("en espera</strong>", self._welcome(en_espera=False))
        self.assertIn("<h5", self._welcome())
        self.assertIsNone(re.search(r"Hola,\s*</h5>", self._welcome()), "sin nombre no queda un «Hola,» colgando")
        self.assertIn("Hola, Ana", self._welcome(nombre="Ana"))

    def test_bienvenida_conserva_lo_escrito_tras_un_error(self):
        html = self._welcome(nombre_in="Ana", apellido_in="Soto", rut="12.345.678-5", phone_in="9 1234 5678", direccion="Av. X 1",
                             comuna="Las Condes", ciudad="Santiago", direccion_lat="-33.4", direccion_lng="-70.5", direccion_place_id="ChIJN1t_tDeuEmsRUsoyG83frY4")
        for v in ("Ana", "Soto", "12.345.678-5", "9 1234 5678", "Av. X 1", "Las Condes", "Santiago", "-33.4", "-70.5", "ChIJN1t_tDeuEmsRUsoyG83frY4"):
            self.assertIn(f'value="{v}"', html)

    def test_bienvenida_carga_los_helpers_compartidos_y_google_solo_si_hay_clave(self):
        sin = self._welcome()
        self.assertIn("ilus_persona_fields.js", sin)
        self.assertIn("ilus_ui.js", sin)
        self.assertNotIn("__ILUS_GMAPS_KEY", sin)
        con = self._welcome(google_maps_api_key="AIzaFAKE")
        self.assertIn("__ILUS_GMAPS_KEY", con)
        self.assertIn("P.comunaCiudad", sin, "la comuna usa el helper compartido, no una copia")

    def test_bienvenida_sin_ventanas_nativas_y_con_los_mensajes_junto_al_campo(self):
        js = _leer("templates", "welcome.html")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", js), "REGLA #1")
        for id_ in ("wMsgRut", "wMsgTel", "wMsgDir"):
            self.assertIn(f'id="{id_}"', js)

    def test_bienvenida_el_telefono_con_prefijo_escrito_tecla_por_tecla_no_pierde_el_final(self):
        """Hallazgo de la prueba en Chromium: tecleando «+56 9 1234 5678» el recorte a 9 dígitos dejaba «5 6912 3456». El prefijo se quita en
        cuanto SOBRAN dígitos (más de 9), no al llegar a 11."""
        t = _leer("templates", "welcome.html")
        self.assertIn("if (d.length > 9 && d.indexOf('56') === 0) d = d.slice(2);", t)
        self.assertNotIn("d.length >= 11 && d.indexOf('56')", t)

    def test_bienvenida_requisitos_de_clave_legibles(self):
        """Antes #555 sobre #111 (2,5:1) y un título de 10,9 px."""
        t = _leer("templates", "welcome.html")
        self.assertNotIn(".pw-req { color:#555", t)
        self.assertIn(".pw-req { color:#9ca3af", t)
        self.assertIn(".pw-req-title { color:#9ca3af;font-size:.72rem", t)

    # —— formulario de alta ——
    def test_alta_nueva_arranca_en_invitacion_rapida(self):
        html = self._form()
        self.assertIn('<input type="hidden" name="modo" id="ufModo" value="invitar">', html)
        self.assertIn("uf-modo-card", html)
        self.assertIn("Enviar invitación", html)
        self.assertIn("Prefiero completar todos los datos yo mismo (avanzado)", html)

    def test_alta_conserva_el_modo_completo_tras_un_error(self):
        class Multi(dict):
            pass
        html = self._form(errors=["x"], fd=Multi(modo="completo", username="a@b.cl"))
        self.assertIn('name="modo" id="ufModo" value="completo"', html)

    def test_el_modo_rapido_esconde_solo_lo_que_la_persona_declara_por_si_misma(self):
        html = self._form()
        self.assertEqual(html.count("col-md-6 uf-solo-completo"), 2, "nombre y teléfono")
        self.assertEqual(html.count("col-12 uf-solo-completo"), 1, "clave manual de emergencia (superadmin)")
        self.assertEqual(html.count("col-md-4 uf-solo-completo"), 1, "estado")
        self.assertEqual(html.count("ilus-card mb-3 uf-solo-completo"), 2, "datos personales e invitación")
        # el correo y el cargo (rol) NO se esconden jamás
        self.assertIn('id="userEmailInput"', html)
        self.assertNotRegex(html, r'uf-solo-completo[^>]*>\s*<div class="uf-step-head">\s*<span class="uf-step-circle" id="ufCircle-email"')
        self.assertNotRegex(html, r'class="[^"]*uf-solo-completo[^"]*">\s*(?:<div[^>]*>\s*){0,2}<div class="uf-role-box"')
        self.assertIn("Cargo (rol del sistema)", html)

    def test_el_css_del_modo_rapido(self):
        html = self._form()
        self.assertIn(".uf-rapido .uf-solo-completo{display:none !important}", html)
        self.assertIn("body:not(.uf-rapido) .uf-solo-rapido{display:none !important}", html)

    def test_editar_no_muestra_nada_del_modo_rapido(self):
        t = _leer("templates", "user_form.html")
        # la tarjeta del modo y el campo `modo` solo viven dentro de {% if not user %}
        bloque = t.split("{% if not user %}")[1].split("{% endif %}")[0]
        self.assertIn('name="modo"', bloque)
        self.assertIn("uf-modo-card", bloque)
        self.assertNotIn('name="modo"', t.replace(bloque, ""))

    # —— lista de usuarios ——
    def test_la_lista_marca_a_los_que_estan_en_espera_y_permite_reenviar(self):
        t = _leer("templates", "users.html")
        self.assertIn("{% set _espera = (not user.active) and user.requires_password_change and not user.last_login_at %}", t)
        self.assertIn("En espera de registro", t)
        self.assertIn("reenviarInvitacion(", t)
        self.assertIn("/admin/users/${uid}/invite", t)
        sin_helpers = t.replace("ilusAlert(", "").replace("ilusConfirm(", "").replace("ilusPrompt(", "").replace("nunca alert() nativo", "")
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", sin_helpers), "REGLA #1")

    def test_la_regla_de_la_lista_es_la_misma_que_la_del_servidor(self):
        """Misma condición en Python y en la plantilla: inactivo + pide clave + nunca entró."""
        for fila, esperado in (({"active": 0, "requires_password_change": 1, "last_login_at": None}, True),
                               ({"active": 1, "requires_password_change": 1, "last_login_at": None}, False),
                               ({"active": 0, "requires_password_change": 0, "last_login_at": None}, False),
                               ({"active": 0, "requires_password_change": 1, "last_login_at": "x"}, False)):
            tpl = (not fila["active"]) and fila["requires_password_change"] and not fila["last_login_at"]
            self.assertEqual(bool(tpl), esperado)
            self.assertEqual(ur.esta_en_espera(fila), esperado)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""🔐 PASSKEYS (Face ID / huella) · rutas, base de datos y sesión (Daniel, 05-nov-2026): «que desbloquee la aplicación con la huella… y dejarlo
habilitado por el front (Mi cuenta)».

Decisiones que se vigilan:
  1. ES UN MODO ADICIONAL: la contraseña no se toca. Nadie queda sin poder entrar por esto.
  2. Se activa y se apaga DISPOSITIVO POR DISPOSITIVO, por la propia persona (la gestión superadmin también puede quitar uno).
  3. El ingreso crea la MISMA sesión que la contraseña y deja auditoría «login_ok» con método «passkey»; un usuario desactivado o una llave
     quitada no entran; el desafío es de un solo uso y vence; el contador repetido delata una copia.
  4. `next` solo puede ser una ruta de esta app. ILUS_PASSKEYS=0 apaga todo (404).
Correr:  python -m pytest tests/test_passkeys_modulo.py -q -p no:warnings
"""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, g, redirect, session  # noqa: E402

import passkeys_module  # noqa: E402
import passkeys_reglas as pk  # noqa: E402
from _passkeys_falso import ORIGEN, Autenticador, BaseFalsa  # noqa: E402

USUARIOS = {
    1: {"id": 1, "username": "ana@ilus.test", "nombre": "Ana Pérez", "role": "ejecutivo", "active": 1},
    2: {"id": 2, "username": "beto@ilus.test", "nombre": "Beto Soto", "role": "tecnico", "active": 1},
    3: {"id": 3, "username": "dani@ilus.test", "nombre": "Daniel Aguilar", "role": "superadmin", "active": 1},
    4: {"id": 4, "username": "caro@ilus.test", "nombre": "Caro Mora", "role": "ejecutivo", "active": 0},
}


def _app(base):
    app = Flask(__name__)
    app.secret_key = "solo-pruebas"
    app.config["TESTING"] = True
    auditoria = []

    @app.before_request
    def _cargar():
        uid = session.get("user_id")
        g.user = dict(USUARIOS[uid]) if uid in USUARIOS and USUARIOS[uid]["active"] else None

    def login_required(vista):
        from functools import wraps

        @wraps(vista)
        def w(*a, **k):
            if not g.user:
                return redirect("/login")
            return vista(*a, **k)
        return w

    def audit(accion, **k):
        auditoria.append((accion, k))

    ctx = {"mysql_fetchone": base.fetchone, "mysql_fetchall": base.fetchall, "mysql_execute": base.execute,
           "mysql_execute_returning_rowcount": base.rowcount, "login_required": login_required, "AUTH_TABLE": "app_users",
           "get_auth_user_by_id": lambda uid: dict(USUARIOS[uid]) if uid in USUARIOS else None, "_audit": audit,
           "_public_base_url": lambda: ORIGEN}
    passkeys_module.register_passkey_routes(app, ctx)

    @app.route("/_sesion/<int:uid>")
    def _sesion(uid):
        session["user_id"] = uid
        session["otra_cosa"] = "no debe sobrevivir al ingreso"
        return "ok"

    @app.route("/_quien")
    def _quien():
        return {"uid": session.get("user_id"), "otra": session.get("otra_cosa")}

    app.auditoria = auditoria
    return app


class Base(unittest.TestCase):
    def setUp(self):
        self.base = BaseFalsa()
        self.app = _app(self.base)
        self.c = self.app.test_client()

    def entrar_como(self, uid, cliente=None):
        (cliente or self.c).get(f"/_sesion/{uid}")

    def registrar(self, uid=1, a=None, cliente=None, **k):
        """El teléfono activa Face ID: opciones → firma el registro → verificar. → (respuesta, autenticador)."""
        c = cliente or self.c
        self.entrar_como(uid, c)
        o = c.post("/passkeys/registro/opciones", json={}).get_json()
        a = a or Autenticador()
        r = c.post("/passkeys/registro/verificar", json=a.registro(o["publicKey"]["challenge"], **k))
        return r, a

    def ingresar(self, a, cliente=None, contador=0, siguiente=None, **k):
        """Un teléfono SIN sesión entra con Face ID: opciones → firma → verificar. → respuesta."""
        c = cliente or self.app.test_client()
        o = c.post("/passkeys/login/opciones", json={}).get_json()
        cuerpo = a.ingreso(o["publicKey"]["challenge"], contador=contador, **k)
        if siguiente is not None:
            cuerpo["next"] = siguiente
        r = c.post("/passkeys/login/verificar", json=cuerpo)
        r.cliente = c
        return r


# ══════════════════════════════════════════════════════════════════
#  1. Activar Face ID en un dispositivo (Mi cuenta)
# ══════════════════════════════════════════════════════════════════
class TestRegistro(Base):
    def test_hay_que_haber_entrado_con_la_contraseña_para_activarlo(self):
        for ruta in ("/passkeys/registro/opciones", "/passkeys/registro/verificar"):
            self.assertEqual(self.c.post(ruta, json={}).status_code, 302, ruta)             # al login, como cualquier otra página
        self.assertEqual(self.c.get("/passkeys/mis").status_code, 302)

    def test_las_opciones_piden_verificar_la_identidad_en_el_propio_telefono(self):
        self.entrar_como(1)
        p = self.c.post("/passkeys/registro/opciones", json={}).get_json()["publicKey"]
        self.assertEqual(p["rp"]["id"], "ilus.test")
        sel = p["authenticatorSelection"]
        self.assertEqual((sel["authenticatorAttachment"], sel["userVerification"]), ("platform", "required"))     # Face ID/huella del teléfono, no una llave USB
        self.assertEqual(p["attestation"], "none")
        self.assertEqual(sorted(x["alg"] for x in p["pubKeyCredParams"]), [-257, -8, -7])
        self.assertEqual(p["excludeCredentials"], [])
        self.assertEqual(pk.b64u_dec(p["user"]["id"]), b"1")
        self.assertGreaterEqual(len(pk.b64u_dec(p["challenge"])), 32)

    def test_activar_guarda_la_llave_publica_y_deja_constancia(self):
        r, a = self.registrar()
        self.assertEqual(r.status_code, 200, r.get_json())
        j = r.get_json()
        self.assertEqual(len(j["dispositivos"]), 1)
        self.assertEqual(j["dispositivos"][0]["etiqueta"], "iPhone · Safari")
        self.assertEqual(j["credencial"], a.id_b64)
        fila = self.base.filas[1]
        self.assertEqual((fila["user_id"], fila["credential_id"], fila["public_key"], fila["alg"]), (1, a.id_b64, a.publica_b64, -7))
        self.assertNotIn("private", str(fila).lower())                                      # nunca se guarda la llave privada
        self.assertEqual(self.app.auditoria[-1][0], "passkey_registro")

    def test_los_dispositivos_de_cada_persona_son_suyos(self):
        self.registrar(1)
        self.entrar_como(2)
        self.assertEqual(self.c.get("/passkeys/mis").get_json()["dispositivos"], [])
        self.entrar_como(1)
        self.assertEqual(len(self.c.get("/passkeys/mis").get_json()["dispositivos"]), 1)

    def test_el_mismo_dispositivo_no_se_activa_dos_veces(self):
        r, a = self.registrar()
        r2, _ = self.registrar(a=a)
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(len(self.base.filas), 1)
        o = self.c.post("/passkeys/registro/opciones", json={}).get_json()["publicKey"]
        self.assertEqual([x["id"] for x in o["excludeCredentials"]], [a.id_b64])           # el teléfono ni siquiera lo ofrece de nuevo

    def test_lo_que_no_cuadra_no_se_guarda(self):
        for k, codigo in (({"origen": "https://sitio-falso.com"}, "ORIGEN"), ({"flags": 0x01}, "VERIFICACION"), ({"rp_id": "otro.com"}, "DOMINIO"),
                          ({"tipo": "webauthn.get"}, "TIPO")):
            r, _ = self.registrar(**k)
            self.assertEqual(r.status_code, 400, k)
            self.assertEqual(r.get_json()["error_codigo"], codigo)
        self.assertEqual(self.base.filas, {})
        self.assertTrue(all(x[0] == "passkey_registro_fallo" for x in self.app.auditoria))

    def test_el_desafio_del_registro_es_de_un_solo_uso(self):
        self.entrar_como(1)
        o = self.c.post("/passkeys/registro/opciones", json={}).get_json()
        a = Autenticador()
        cuerpo = a.registro(o["publicKey"]["challenge"])
        self.assertEqual(self.c.post("/passkeys/registro/verificar", json=cuerpo).status_code, 200)
        self.assertEqual(self.c.post("/passkeys/registro/verificar", json=a.registro(o["publicKey"]["challenge"])).status_code, 400)

    def test_sin_pedir_opciones_no_hay_desafio(self):
        self.entrar_como(1)
        r = self.c.post("/passkeys/registro/verificar", json=Autenticador().registro(pk.b64u_enc(b"d" * 32)))
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (400, "DESAFIO"))

    def test_hay_un_tope_de_dispositivos_por_persona(self):
        for _ in range(pk.MAX_POR_USUARIO - 1):
            r, _a = self.registrar()
            self.assertEqual(r.status_code, 200)
        self.entrar_como(1)
        o = self.c.post("/passkeys/registro/opciones", json={}).get_json()          # con 9 todavía deja pedir el décimo
        a = Autenticador()
        self.assertEqual(self.c.post("/passkeys/registro/verificar", json=a.registro(o["publicKey"]["challenge"])).status_code, 200)
        self.assertEqual(self.c.post("/passkeys/registro/opciones", json={}).status_code, 409)       # con 10, ya no
        # y si dos pestañas pidieron opciones a la vez, el tope se vuelve a comprobar al guardar
        otro = Autenticador()
        extra = self.base.filas[1].copy()
        extra.update(id=999, credential_id="otra-credencial-cualquiera")
        self.base.filas[999] = extra
        self.assertEqual(len([f for f in self.base.filas.values() if f["user_id"] == 1 and f["revocada_at"] is None]), pk.MAX_POR_USUARIO + 1)
        self.c.post("/passkeys/registro/opciones", json={})                           # 409: sin desafío nuevo
        self.assertEqual(self.c.post("/passkeys/registro/verificar", json=otro.registro(o["publicKey"]["challenge"])).status_code, 400)

    def test_datos_que_no_son_json_no_rompen_nada(self):
        self.entrar_como(1)
        self.c.post("/passkeys/registro/opciones", json={})
        for cuerpo in ({}, {"id": 5}, {"publicKeyAlgorithm": "x"}):
            r = self.c.post("/passkeys/registro/verificar", json=cuerpo)
            self.assertEqual(r.status_code, 400)


# ══════════════════════════════════════════════════════════════════
#  2. Entrar con Face ID (sin sesión)
# ══════════════════════════════════════════════════════════════════
class TestIngreso(Base):
    def setUp(self):
        super().setUp()
        _r, self.tel = self.registrar(1)

    def test_las_opciones_se_piden_sin_sesion_y_exigen_verificar_la_identidad(self):
        c = self.app.test_client()
        r = c.post("/passkeys/login/opciones", json={})
        self.assertEqual(r.status_code, 200)
        p = r.get_json()["publicKey"]
        self.assertEqual((p["rpId"], p["userVerification"]), ("ilus.test", "required"))

    def test_entra_con_la_misma_sesion_que_la_contraseña(self):
        c = self.app.test_client()
        c.get("/_sesion/2")                                                                  # una sesión anterior, de OTRA persona, queda borrada
        r = self.ingresar(self.tel, cliente=c, contador=0)
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual(r.get_json()["redirect"], "/")
        self.assertEqual(c.get("/_quien").get_json(), {"uid": 1, "otra": None})
        with c.session_transaction() as s:
            self.assertTrue(s.permanent)                                                     # mismo tiempo de inactividad (PERMANENT_SESSION_LIFETIME)

    def test_queda_en_la_auditoria_y_en_el_ultimo_ingreso(self):
        self.ingresar(self.tel)
        accion, k = self.app.auditoria[-1]
        self.assertEqual(accion, "login_ok")
        self.assertEqual((k["details"]["metodo"], k["target_id"], k["user_override"]["username"]), ("passkey", 1, "ana@ilus.test"))
        self.assertEqual(len(self.base.ultimo_login), 1)
        fila = self.base.filas[1]
        self.assertIsNotNone(fila["ultimo_uso_at"])

    def test_next_solo_puede_ser_una_ruta_de_esta_app(self):
        for pedido, esperado in (("/mi-cuenta", "/mi-cuenta"), ("//sitio-malo.com", "/"), ("https://sitio-malo.com", "/"), ("/\\sitio.com", "/"),
                                 (None, "/"), (5, "/")):
            r = self.ingresar(self.tel, siguiente=pedido)
            self.assertEqual(r.get_json()["redirect"], esperado, pedido)
            # el contador del teléfono de mentira no avanza (los iPhone mandan siempre 0): se puede repetir sin problema

    def test_el_desafio_es_de_un_solo_uso_y_una_firma_capturada_no_sirve_dos_veces(self):
        c = self.app.test_client()
        o = c.post("/passkeys/login/opciones", json={}).get_json()
        cuerpo = self.tel.ingreso(o["publicKey"]["challenge"])
        self.assertEqual(c.post("/passkeys/login/verificar", json=cuerpo).status_code, 200)
        otro = self.app.test_client()                                                        # otro navegador con la misma firma capturada
        r = otro.post("/passkeys/login/verificar", json=cuerpo)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(otro.get("/_quien").get_json()["uid"], None)

    def test_un_intento_fallido_quema_el_desafio(self):
        """Aunque el primer intento falle, el desafío ya no vale: no se puede probar firmas una tras otra sobre el mismo."""
        c = self.app.test_client()
        o = c.post("/passkeys/login/opciones", json={}).get_json()
        malo = self.tel.ingreso(o["publicKey"]["challenge"], firmar_con=Autenticador())
        self.assertEqual(c.post("/passkeys/login/verificar", json=malo).status_code, 401)
        r = c.post("/passkeys/login/verificar", json=self.tel.ingreso(o["publicKey"]["challenge"]))
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "DESAFIO"))
        self.assertIsNone(c.get("/_quien").get_json()["uid"])

    def test_el_desafio_vence_a_los_dos_minutos(self):
        c = self.app.test_client()
        o = c.post("/passkeys/login/opciones", json={}).get_json()
        cuerpo = self.tel.ingreso(o["publicKey"]["challenge"])
        with mock.patch("passkeys_reglas.time.time", return_value=__import__("time").time() + pk.DESAFIO_TTL_S + 5):
            r = c.post("/passkeys/login/verificar", json=cuerpo)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "DESAFIO"))

    def test_sin_pedir_opciones_antes_no_entra(self):
        c = self.app.test_client()
        r = c.post("/passkeys/login/verificar", json=self.tel.ingreso(pk.b64u_enc(b"d" * 32)))
        self.assertEqual(r.status_code, 401)

    def test_firma_de_otro_telefono_no_entra(self):
        r = self.ingresar(self.tel, firmar_con=Autenticador())
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "FIRMA"))
        self.assertIsNone(r.cliente.get("/_quien").get_json()["uid"])
        self.assertEqual(self.app.auditoria[-1][0], "login_fail")
        self.assertEqual(self.app.auditoria[-1][1]["details"]["metodo"], "passkey")

    def test_origen_o_sitio_equivocado_no_entra(self):
        for k, codigo in (({"origen": "https://sitio-falso.com"}, "ORIGEN"), ({"rp_id": "otro.com"}, "DOMINIO"), ({"flags": 0x01}, "VERIFICACION")):
            r = self.ingresar(self.tel, **k)
            self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, codigo), k)

    def test_un_dispositivo_que_ilus_no_conoce_se_explica_con_calma(self):
        r = self.ingresar(Autenticador())
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "DESCONOCIDA"))
        self.assertIn("Mi cuenta", r.get_json()["error"])

    def test_el_usuario_desactivado_no_entra_aunque_conserve_su_llave(self):
        _r, tel4 = self.registrar(1, a=Autenticador())
        self.base.filas[2]["user_id"] = 4                                                   # esa llave es de una persona desactivada
        r = self.ingresar(tel4)
        self.assertEqual(r.status_code, 401)
        self.assertIsNone(r.cliente.get("/_quien").get_json()["uid"])

    def test_la_llave_quitada_no_entra(self):
        self.entrar_como(1)
        self.assertEqual(self.c.post("/passkeys/1/quitar", json={}).status_code, 200)
        r = self.ingresar(self.tel)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "DESCONOCIDA"))

    def test_el_contador_repetido_delata_una_copia(self):
        self.assertEqual(self.ingresar(self.tel, contador=5).status_code, 200)
        self.assertEqual(self.base.filas[1]["sign_count"], 5)
        r = self.ingresar(self.tel, contador=5)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "CLON"))
        self.assertEqual(self.ingresar(self.tel, contador=6).status_code, 200)
        self.assertEqual(self.base.filas[1]["sign_count"], 6)

    def test_el_usuario_que_dice_el_telefono_debe_ser_el_de_la_llave(self):
        r = self.ingresar(self.tel, user_handle=pk.b64u_enc(b"2"))                          # la llave es de la persona 1
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (401, "USUARIO"))
        self.assertEqual(self.ingresar(self.tel, user_handle=pk.b64u_enc(b"1")).status_code, 200)

    def test_datos_malos_no_rompen_ni_entran(self):
        c = self.app.test_client()
        c.post("/passkeys/login/opciones", json={})
        for cuerpo in ({}, {"id": None}, {"id": "con espacios"}, {"id": self.tel.id_b64}):
            self.assertEqual(c.post("/passkeys/login/verificar", json=cuerpo).status_code, 401, cuerpo)
            c.post("/passkeys/login/opciones", json={})
        self.assertIsNone(c.get("/_quien").get_json()["uid"])


# ══════════════════════════════════════════════════════════════════
#  3. Quitar un dispositivo (Mi cuenta)
# ══════════════════════════════════════════════════════════════════
class TestQuitar(Base):
    def setUp(self):
        super().setUp()
        self.registrar(1)

    def test_la_persona_quita_su_dispositivo(self):
        self.entrar_como(1)
        r = self.c.post("/passkeys/1/quitar", json={})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["dispositivos"], [])
        self.assertEqual((self.base.filas[1]["revocada_por"], self.app.auditoria[-1][0]), ("ana@ilus.test", "passkey_quitada"))
        self.assertEqual(self.c.post("/passkeys/1/quitar", json={}).status_code, 404)           # ya no está

    def test_nadie_quita_el_dispositivo_de_otra_persona(self):
        self.entrar_como(2)
        self.assertEqual(self.c.post("/passkeys/1/quitar", json={}).status_code, 403)
        self.assertIsNone(self.base.filas[1]["revocada_at"])

    def test_el_superadministrador_si_puede_cortar_un_acceso(self):
        self.entrar_como(3)
        self.assertEqual(self.c.post("/passkeys/1/quitar", json={}).status_code, 200)
        self.assertIsNotNone(self.base.filas[1]["revocada_at"])

    def test_un_dispositivo_inexistente(self):
        self.entrar_como(1)
        self.assertEqual(self.c.post("/passkeys/99/quitar", json={}).status_code, 404)


# ══════════════════════════════════════════════════════════════════
#  4. Interruptor general y huella en el código
# ══════════════════════════════════════════════════════════════════
class TestInterruptor(Base):
    def test_encendido_por_defecto(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ILUS_PASSKEYS", None)
            self.assertTrue(passkeys_module.activas())

    def test_ilus_passkeys_cero_apaga_todo(self):
        self.entrar_como(1)
        with mock.patch.dict(os.environ, {"ILUS_PASSKEYS": "0"}):
            self.assertFalse(passkeys_module.activas())
            for metodo, ruta in (("get", "/passkeys/mis"), ("post", "/passkeys/registro/opciones"), ("post", "/passkeys/registro/verificar"),
                                 ("post", "/passkeys/login/opciones"), ("post", "/passkeys/login/verificar"), ("post", "/passkeys/1/quitar")):
                r = getattr(self.c, metodo)(ruta, **({} if metodo == "get" else {"json": {}}))
                self.assertEqual(r.status_code, 404, ruta)
            with self.app.test_request_context("/"):
                self.assertFalse(self.app.jinja_env.globals.get("passkeys_activas", False))

    def test_el_contexto_de_las_plantillas_dice_si_esta_activo(self):
        with self.app.test_request_context("/"):
            self.app.update_template_context(ctx := {})
            self.assertTrue(ctx["passkeys_activas"])
        with mock.patch.dict(os.environ, {"ILUS_PASSKEYS": "0"}):
            with self.app.test_request_context("/"):
                self.app.update_template_context(ctx := {})
                self.assertFalse(ctx["passkeys_activas"])

    def test_la_llave_privada_no_existe_en_el_servidor(self):
        codigo = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "passkeys_module.py"), encoding="utf-8").read()
        self.assertNotIn("private_key", codigo.lower())
        self.assertNotIn("password", codigo.lower().replace("contraseña", ""))               # este módulo no toca contraseñas


if __name__ == "__main__":
    unittest.main()

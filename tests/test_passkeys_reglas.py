# -*- coding: utf-8 -*-
"""🔐 PASSKEYS (Face ID / huella) · reglas de WebAuthn con FIRMAS REALES (Daniel, 05-nov-2026).

Un «teléfono» de mentira (tests/_passkeys_falso.py) genera llaves de verdad (ES256, RS256 y Ed25519), firma de verdad, y estas pruebas comprueban que
ILUS acepta lo correcto y rechaza cada cosa que NO debe aceptar: otro origen, otro desafío, otro sitio, sin verificar la identidad, firma ajena, datos
alterados, contador repetido, llaves de tipo equivocado y datos mal formados.
Correr:  python -m pytest tests/test_passkeys_reglas.py -q -p no:warnings
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402

import passkeys_reglas as pk  # noqa: E402
from _passkeys_falso import ORIGEN, RP_ID, Autenticador  # noqa: E402

ORIGENES = {ORIGEN}
DESAFIO = pk.b64u_enc(b"d" * 32)


def verificar_reg(a, desafio=DESAFIO, **k):
    campos = a.registro(desafio, **k.pop("registro", {}))
    base = dict(client_data_b64=campos["clientDataJSON"], auth_data_b64=campos["authenticatorData"], publica_b64=campos["publicKey"],
                alg=campos["publicKeyAlgorithm"], id_b64=campos["id"], desafio_b64=desafio, origenes=ORIGENES, rp_id=RP_ID)
    base.update(k)
    return pk.verificar_registro(**base)


def verificar_ing(a, desafio=DESAFIO, guardado=0, **k):
    campos = a.ingreso(desafio, **k.pop("ingreso", {}))
    base = dict(client_data_b64=campos["clientDataJSON"], auth_data_b64=campos["authenticatorData"], firma_b64=campos["signature"], desafio_b64=desafio,
                origenes=ORIGENES, rp_id=RP_ID, publica_b64=a.publica_b64, alg=a.alg, contador_guardado=guardado)
    base.update(k)
    return pk.verificar_ingreso(**base)


class TestRegistro(unittest.TestCase):
    def test_los_tres_algoritmos_de_los_telefonos_se_aceptan(self):
        for alg in (pk.ALG_ES256, pk.ALG_RS256, pk.ALG_EDDSA):
            a = Autenticador(alg)
            r = verificar_reg(a)
            self.assertEqual((r["credential_id"], r["alg"], r["publica"]), (a.id_b64, alg, a.publica_b64), alg)

    def rechaza(self, codigo, a=None, **k):
        with self.assertRaises(pk.PasskeyError) as cm:
            verificar_reg(a or Autenticador(), **k)
        self.assertEqual(cm.exception.codigo, codigo)
        self.assertTrue(cm.exception.mensaje)

    def test_otro_desafio(self):
        self.rechaza("DESAFIO", desafio_b64=pk.b64u_enc(b"otro" * 8))

    def test_otro_origen(self):
        self.rechaza("ORIGEN", registro={"origen": "https://sitio-falso.com"})

    def test_pagina_dentro_de_otra_pagina(self):
        self.rechaza("ORIGEN", registro={"cross": True})

    def test_la_operacion_no_es_un_registro(self):
        self.rechaza("TIPO", registro={"tipo": "webauthn.get"})

    def test_credencial_de_otro_sitio(self):
        self.rechaza("DOMINIO", registro={"rp_id": "otro-sitio.com"})

    def test_sin_verificar_la_identidad_no_se_registra(self):
        self.rechaza("VERIFICACION", registro={"flags": 0x01})                      # presencia sí, Face ID/huella no
        self.rechaza("PRESENCIA", registro={"flags": 0x04})

    def test_sin_datos_de_la_credencial(self):
        self.rechaza("DATOS", registro={"con_credencial": False})

    def test_el_id_debe_ser_el_de_la_credencial(self):
        self.rechaza("DATOS", registro={"id_b64": pk.b64u_enc(b"otro-id")})
        self.rechaza("DATOS", registro={"id_b64": ""})

    def test_llaves_de_tipo_equivocado_o_debiles(self):
        self.rechaza("ALGORITMO", registro={"alg": -65535})
        self.rechaza("CLAVE", Autenticador(pk.ALG_ES256, curva=ec.SECP384R1()))     # ES256 exige P-256
        self.rechaza("CLAVE", Autenticador(pk.ALG_RS256, bits=1024))                # RSA chico
        self.rechaza("CLAVE", registro={"alg": pk.ALG_RS256})                        # dice RSA pero la llave es EC

    def test_datos_mal_formados(self):
        self.rechaza("DATOS", registro={"recortar": 20})
        for malo in (None, "con espacios", "con+mas/signos", "abcde", "ñ"):
            with self.assertRaises(pk.PasskeyError):
                pk.b64u_dec(malo)
        with self.assertRaises(pk.PasskeyError):
            pk.verificar_registro(client_data_b64=pk.b64u_enc(b"no es json"), auth_data_b64="", publica_b64="", alg=-7, id_b64="", desafio_b64=DESAFIO,
                                  origenes=ORIGENES, rp_id=RP_ID)


class TestIngreso(unittest.TestCase):
    def test_los_tres_algoritmos_firman_y_se_aceptan(self):
        for alg in (pk.ALG_ES256, pk.ALG_RS256, pk.ALG_EDDSA):
            a = Autenticador(alg)
            self.assertEqual(verificar_ing(a, ingreso={"contador": 0}), 0, alg)

    def rechaza(self, codigo, a=None, **k):
        with self.assertRaises(pk.PasskeyError) as cm:
            verificar_ing(a or Autenticador(), **k)
        self.assertEqual(cm.exception.codigo, codigo)

    def test_firma_de_otro_telefono(self):
        ajeno = Autenticador()
        self.rechaza("FIRMA", ingreso={"firmar_con": ajeno})

    def test_datos_alterados_despues_de_firmar(self):
        self.rechaza("FIRMA", ingreso={"alterar_client": lambda cd: cd.replace(b'"origin"', b'"extra": 1, "origin"')})
        a = Autenticador()
        campos = a.ingreso(DESAFIO, contador=3)
        # un auth_data distinto del firmado (otro contador)
        otra = a.auth_data(contador=9)
        with self.assertRaises(pk.PasskeyError) as cm:
            pk.verificar_ingreso(client_data_b64=campos["clientDataJSON"], auth_data_b64=pk.b64u_enc(otra), firma_b64=campos["signature"],
                                 desafio_b64=DESAFIO, origenes=ORIGENES, rp_id=RP_ID, publica_b64=a.publica_b64, alg=a.alg, contador_guardado=0)
        self.assertEqual(cm.exception.codigo, "FIRMA")

    def test_desafio_origen_sitio_y_tipo(self):
        self.rechaza("DESAFIO", desafio_b64=pk.b64u_enc(b"x" * 32))
        self.rechaza("ORIGEN", ingreso={"origen": "https://otro.com"})
        self.rechaza("DOMINIO", ingreso={"rp_id": "otro.com"})
        self.rechaza("TIPO", ingreso={"tipo": "webauthn.create"})

    def test_sin_verificar_la_identidad_no_se_entra(self):
        self.rechaza("VERIFICACION", ingreso={"flags": 0x01})
        self.rechaza("PRESENCIA", ingreso={"flags": 0x04})

    def test_el_contador_que_no_avanza_delata_una_copia(self):
        a = Autenticador()
        self.assertEqual(verificar_ing(a, guardado=5, ingreso={"contador": 6}), 6)
        self.rechaza("CLON", a, guardado=5, ingreso={"contador": 5})
        self.rechaza("CLON", a, guardado=5, ingreso={"contador": 3})
        self.rechaza("CLON", a, guardado=5, ingreso={"contador": 0})

    def test_los_telefonos_que_no_cuentan_mandan_siempre_cero_y_eso_es_normal(self):
        a = Autenticador()
        self.assertEqual(verificar_ing(a, guardado=0, ingreso={"contador": 0}), 0)
        self.assertEqual(verificar_ing(a, guardado=0, ingreso={"contador": 7}), 7)

    def test_llave_guardada_dañada(self):
        a = Autenticador()
        with self.assertRaises(pk.PasskeyError):
            verificar_ing(a, publica_b64=pk.b64u_enc(b"no es una llave"))
        with self.assertRaises(pk.PasskeyError):
            verificar_ing(a, alg=-1)


class TestDesafio(unittest.TestCase):
    def test_vale_para_su_proposito_y_dos_minutos(self):
        d = pk.nuevo_desafio("login", ahora=1000)
        self.assertEqual(pk.comprobar_desafio(d, "login", ahora=1000 + pk.DESAFIO_TTL_S), d["c"])
        for ahora, prop in ((1000 + pk.DESAFIO_TTL_S + 1, "login"), (1000, "registro")):
            with self.assertRaises(pk.PasskeyError) as cm:
                pk.comprobar_desafio(d, prop, ahora=ahora)
            self.assertEqual(cm.exception.codigo, "DESAFIO")

    def test_sin_desafio_no_hay_nada_que_comprobar(self):
        for malo in (None, {}, {"p": "login"}, "texto"):
            with self.assertRaises(pk.PasskeyError):
                pk.comprobar_desafio(malo, "login")

    def test_cada_desafio_es_distinto_y_largo(self):
        a, b = pk.nuevo_desafio("login"), pk.nuevo_desafio("login")
        self.assertNotEqual(a["c"], b["c"])
        self.assertGreaterEqual(len(pk.b64u_dec(a["c"])), 32)


class TestDominioYOrigen(unittest.TestCase):
    def test_el_dominio_sale_de_la_direccion_publica(self):
        self.assertEqual(pk.rp_id_de("https://ilus-app-469212710544.southamerica-west1.run.app"), "ilus-app-469212710544.southamerica-west1.run.app")
        self.assertEqual(pk.rp_id_de("http://localhost:5058"), "localhost")
        self.assertEqual(pk.rp_id_de("https://x.com", forzado=" APP.ilusfitness.com "), "app.ilusfitness.com")

    def test_los_origenes_permitidos_son_https_o_localhost(self):
        self.assertEqual(pk.origenes_permitidos("https://a.com/", "https://b.com/, http://evil.com, http://localhost:5000"),
                         {"https://a.com", "https://b.com", "http://localhost:5000"})

    def test_base64url_ida_y_vuelta(self):
        for datos in (b"", b"\x00", b"\xff\xfe\xfd", bytes(range(256))):
            self.assertEqual(pk.b64u_dec(pk.b64u_enc(datos)), datos)
        self.assertNotIn("=", pk.b64u_enc(b"\xff"))


if __name__ == "__main__":
    unittest.main()

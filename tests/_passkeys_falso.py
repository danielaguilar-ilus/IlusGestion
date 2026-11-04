# -*- coding: utf-8 -*-
"""Ayudas de las pruebas de passkeys: un «teléfono» de mentira que firma con llaves REALES (cryptography) y una base de datos en memoria."""
import datetime as dt
import hashlib
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa

import passkeys_reglas as pk

ORIGEN = "https://ilus.test"
RP_ID = "ilus.test"


class Autenticador:
    """El teléfono: guarda la llave privada, entrega la pública y firma lo que se le pide (siempre tras «Face ID»)."""

    def __init__(self, alg=pk.ALG_ES256, bits=2048, curva=None):
        self.alg = alg
        if alg == pk.ALG_ES256:
            self.privada = ec.generate_private_key(curva or ec.SECP256R1())
        elif alg == pk.ALG_RS256:
            self.privada = rsa.generate_private_key(public_exponent=65537, key_size=bits)
        else:
            self.privada = ed25519.Ed25519PrivateKey.generate()
        self.id = os.urandom(32)
        self.user_handle = None

    @property
    def publica_b64(self):
        der = self.privada.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        return pk.b64u_enc(der)

    @property
    def id_b64(self):
        return pk.b64u_enc(self.id)

    def auth_data(self, *, rp_id=RP_ID, flags=0x01 | 0x04, contador=0, con_credencial=False, recortar=None):
        datos = hashlib.sha256(rp_id.encode()).digest() + bytes([flags | (0x40 if con_credencial else 0)]) + contador.to_bytes(4, "big")
        if con_credencial:
            datos += b"\x00" * 16 + len(self.id).to_bytes(2, "big") + self.id + b"\xa0"          # AAGUID + id + «llave COSE» (acá vacía: no se lee)
        return datos[:recortar] if recortar else datos

    @staticmethod
    def client_data(tipo, desafio, origen=ORIGEN, cross=False):
        d = {"type": tipo, "challenge": desafio, "origin": origen}
        if cross:
            d["crossOrigin"] = True
        return json.dumps(d).encode()

    def firmar(self, datos):
        if self.alg == pk.ALG_ES256:
            return self.privada.sign(datos, ec.ECDSA(hashes.SHA256()))
        if self.alg == pk.ALG_RS256:
            return self.privada.sign(datos, padding.PKCS1v15(), hashes.SHA256())
        return self.privada.sign(datos)

    # lo que manda el navegador al registrar (campos de passkeys_module)
    def registro(self, desafio, *, origen=ORIGEN, rp_id=RP_ID, flags=0x01 | 0x04, tipo="webauthn.create", con_credencial=True, cross=False,
                 alg=None, id_b64=None, recortar=None):
        return {"id": self.id_b64 if id_b64 is None else id_b64, "clientDataJSON": pk.b64u_enc(self.client_data(tipo, desafio, origen, cross)),
                "authenticatorData": pk.b64u_enc(self.auth_data(rp_id=rp_id, flags=flags, con_credencial=con_credencial, recortar=recortar)),
                "publicKey": self.publica_b64, "publicKeyAlgorithm": self.alg if alg is None else alg, "transports": ["internal"],
                "etiqueta": "iPhone · Safari"}

    # lo que manda el navegador al entrar
    def ingreso(self, desafio, *, origen=ORIGEN, rp_id=RP_ID, flags=0x01 | 0x04, contador=0, tipo="webauthn.get", firmar_con=None, user_handle=None,
                alterar_client=None, nombre_cred=None):
        cd = self.client_data(tipo, desafio, origen)
        ad = self.auth_data(rp_id=rp_id, flags=flags, contador=contador)
        firma = (firmar_con or self).firmar(ad + hashlib.sha256(cd).digest())
        if alterar_client:
            cd = alterar_client(cd)
        return {"id": nombre_cred or self.id_b64, "clientDataJSON": pk.b64u_enc(cd), "authenticatorData": pk.b64u_enc(ad),
                "signature": pk.b64u_enc(firma), "userHandle": user_handle}


class BaseFalsa:
    """La tabla user_passkeys y lo poco que toca de app_users, en memoria."""

    def __init__(self):
        self.filas, self.sig = {}, 1
        self.auditoria, self.ultimo_login = [], []

    @staticmethod
    def _n(q):
        return " ".join(q.split())

    def fetchone(self, q, p=None):
        r = self.fetchall(q, p)
        return r[0] if r else None

    def fetchall(self, q, p=None):
        s, p = self._n(q), p or ()
        vivas = lambda uid: [f for f in self.filas.values() if f["user_id"] == uid and f["revocada_at"] is None]
        if s.startswith("SELECT credential_id FROM user_passkeys WHERE user_id=%s"):
            return [{"credential_id": f["credential_id"]} for f in vivas(p[0])]
        if s.startswith("SELECT COUNT(*) AS n FROM user_passkeys WHERE user_id=%s"):
            return [{"n": len(vivas(p[0]))}]
        if s.startswith("SELECT id, credential_id, etiqueta, user_agent, creada_at, ultimo_uso_at FROM user_passkeys WHERE user_id=%s"):
            return [dict(f) for f in sorted(vivas(p[0]), key=lambda f: f["id"])]
        if s.startswith("SELECT id, user_id FROM user_passkeys WHERE id=%s AND revocada_at IS NULL"):
            f = self.filas.get(p[0])
            return [{"id": f["id"], "user_id": f["user_id"]}] if f and f["revocada_at"] is None else []
        if s.startswith("SELECT * FROM user_passkeys WHERE credential_id=%s AND revocada_at IS NULL"):
            return [dict(f) for f in self.filas.values() if f["credential_id"] == p[0] and f["revocada_at"] is None]
        return []

    def execute(self, q, p=None):
        s, p = self._n(q), p or ()
        if s.startswith("CREATE TABLE IF NOT EXISTS user_passkeys"):
            return
        if s.startswith("INSERT INTO user_passkeys"):
            uid, cred, pub, alg, cnt, trans, etq, ua = p
            if any(f["credential_id"] == cred for f in self.filas.values()):
                e = Exception("(1062, \"Duplicate entry for key 'uq_passkey_cred'\")")
                e.args = (1062, "Duplicate entry")
                raise e
            self.filas[self.sig] = {"id": self.sig, "user_id": uid, "credential_id": cred, "public_key": pub, "alg": alg, "sign_count": cnt,
                                    "transports": trans, "etiqueta": etq, "user_agent": ua, "creada_at": dt.datetime(2026, 11, 5, 10, 0),
                                    "ultimo_uso_at": None, "revocada_at": None, "revocada_por": None}
            self.sig += 1
            return
        if s.startswith("UPDATE user_passkeys SET sign_count=GREATEST(sign_count, %s)"):
            nuevo, pid = p
            f = self.filas[pid]
            f["sign_count"] = max(f["sign_count"], nuevo)
            f["ultimo_uso_at"] = dt.datetime(2026, 11, 5, 11, 0)
            return
        if s.startswith("UPDATE `app_users` SET last_login_at=NOW()"):
            self.ultimo_login.append(p)
            return
        raise AssertionError("SQL no previsto en la base falsa: " + s[:120])

    def rowcount(self, q, p=None):
        s, p = self._n(q), p or ()
        if s.startswith("UPDATE user_passkeys SET revocada_at=NOW(), revocada_por=%s WHERE id=%s AND revocada_at IS NULL"):
            f = self.filas.get(p[1])
            if not f or f["revocada_at"] is not None:
                return 0
            f.update(revocada_at=dt.datetime(2026, 11, 5, 12, 0), revocada_por=p[0])
            return 1
        raise AssertionError("SQL no previsto en la base falsa: " + s[:120])

# -*- coding: utf-8 -*-
"""🔐 PASSKEYS (Face ID / huella) · reglas puras de WebAuthn — Daniel, 05-nov-2026:
«que desbloquee la aplicación con la huella o los métodos de seguridad que tenga el teléfono… y que lo controle yo por el front».

Sin Flask ni base de datos: solo verifica lo que el navegador entrega, con la librería `cryptography` (ya está en requirements). Así se puede probar
con firmas REALES.

Cómo funciona (el teléfono guarda una llave PRIVADA que nunca sale de él y solo se usa tras Face ID o huella; ILUS guarda la PÚBLICA):
  · Registro: el navegador crea el par de llaves y entrega la pública (SPKI), el id de la credencial y los datos del autenticador. Acá se comprueba que
    venga de ESTA página (origen), que responda al desafío que se le dio, que el usuario se verificó (Face ID/huella) y que el id sea el mismo.
  · Ingreso: el navegador firma el desafío. Acá se comprueba el origen, el desafío (uso único y con vencimiento), el hash del dominio, que haya habido
    verificación del usuario, la FIRMA con la llave pública guardada y que el contador no retroceda (clon).
Los tres algoritmos que usan los teléfonos: ES256 (ECDSA P-256), RS256 (RSA PKCS#1 v1.5) y EdDSA (Ed25519).
"""
import base64
import binascii
import hashlib
import json
import re
import secrets
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa

ALG_ES256, ALG_RS256, ALG_EDDSA = -7, -257, -8
ALGORITMOS = (ALG_ES256, ALG_RS256, ALG_EDDSA)
DESAFIO_TTL_S = 120                 # el desafío vale 2 minutos y una sola vez
MAX_POR_USUARIO = 10                # dispositivos con Face ID/huella por persona
FLAG_UP, FLAG_UV, FLAG_AT = 0x01, 0x04, 0x40

_B64U = re.compile(r"^[A-Za-z0-9_-]*$")


class PasskeyError(Exception):
    """Un motivo claro (código + frase para la persona) por el cual NO se acepta lo que entregó el navegador."""

    def __init__(self, codigo, mensaje):
        super().__init__(mensaje)
        self.codigo, self.mensaje = codigo, mensaje


# ── base64url (sin relleno), estricto ───────────────────────────────────────
def b64u_enc(datos):
    return base64.urlsafe_b64encode(bytes(datos)).rstrip(b"=").decode("ascii")


def b64u_dec(texto):
    if not isinstance(texto, str) or not _B64U.match(texto) or len(texto) % 4 == 1:
        raise PasskeyError("DATOS", "Los datos del dispositivo llegaron incompletos. Intenta de nuevo.")
    try:
        return base64.urlsafe_b64decode(texto + "=" * (-len(texto) % 4))
    except (binascii.Error, ValueError):
        raise PasskeyError("DATOS", "Los datos del dispositivo llegaron incompletos. Intenta de nuevo.")


# ── Desafío: uso único y con vencimiento ────────────────────────────────────
def nuevo_desafio(proposito, ahora=None):
    return {"c": b64u_enc(secrets.token_bytes(32)), "t": int(ahora if ahora is not None else time.time()), "p": proposito}


def comprobar_desafio(guardado, proposito, ahora=None):
    """El desafío entregado a ESTA persona para este propósito y vigente. → su texto base64url."""
    ahora = int(ahora if ahora is not None else time.time())
    if not isinstance(guardado, dict) or guardado.get("p") != proposito or not guardado.get("c"):
        raise PasskeyError("DESAFIO", "La verificación venció o ya se usó. Intenta de nuevo.")
    if ahora - int(guardado.get("t") or 0) > DESAFIO_TTL_S:
        raise PasskeyError("DESAFIO", "La verificación venció. Intenta de nuevo.")
    return guardado["c"]


# ── Lo que entrega el navegador ────────────────────────────────────────────
def _client_data(client_data_b64, tipo, desafio_b64, origenes):
    crudo = b64u_dec(client_data_b64)
    try:
        cd = json.loads(crudo.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise PasskeyError("DATOS", "Los datos del dispositivo no se entendieron. Intenta de nuevo.")
    if not isinstance(cd, dict) or cd.get("type") != tipo:
        raise PasskeyError("TIPO", "El dispositivo respondió a otra operación. Intenta de nuevo.")
    if not secrets.compare_digest(str(cd.get("challenge") or ""), str(desafio_b64)):
        raise PasskeyError("DESAFIO", "La verificación no coincide con la que se pidió. Intenta de nuevo.")
    if cd.get("origin") not in set(origenes or ()):
        raise PasskeyError("ORIGEN", "La verificación no viene de esta página.")
    if cd.get("crossOrigin") is True:
        raise PasskeyError("ORIGEN", "La verificación no viene de esta página.")
    return crudo


def parsear_auth_data(auth):
    """authenticatorData → {rp_hash, flags, contador, credencial (id en bytes o None)}."""
    if len(auth) < 37:
        raise PasskeyError("DATOS", "Los datos del dispositivo llegaron incompletos. Intenta de nuevo.")
    out = {"rp_hash": auth[:32], "flags": auth[32], "contador": int.from_bytes(auth[33:37], "big"), "credencial": None}
    if out["flags"] & FLAG_AT:
        if len(auth) < 55:
            raise PasskeyError("DATOS", "Los datos del dispositivo llegaron incompletos. Intenta de nuevo.")
        largo = int.from_bytes(auth[53:55], "big")
        if largo < 1 or len(auth) < 55 + largo:
            raise PasskeyError("DATOS", "Los datos del dispositivo llegaron incompletos. Intenta de nuevo.")
        out["credencial"] = auth[55:55 + largo]
    return out


def _auth_data(auth_b64, rp_id, exigir_at=False):
    auth = b64u_dec(auth_b64)
    ad = parsear_auth_data(auth)
    if not secrets.compare_digest(ad["rp_hash"], hashlib.sha256(rp_id.encode("utf-8")).digest()):
        raise PasskeyError("DOMINIO", "La verificación no corresponde a este sitio.")
    if not ad["flags"] & FLAG_UP:
        raise PasskeyError("PRESENCIA", "No se confirmó la presencia en el dispositivo. Intenta de nuevo.")
    if not ad["flags"] & FLAG_UV:
        raise PasskeyError("VERIFICACION", "El dispositivo no verificó que eres tú (Face ID, huella o código). Intenta de nuevo.")
    if exigir_at and not ad["credencial"]:
        raise PasskeyError("DATOS", "El dispositivo no entregó su credencial. Intenta de nuevo.")
    return auth, ad


# ── Llaves públicas ─────────────────────────────────────────────────────────
def cargar_clave(publica_b64, alg):
    """La llave pública (SPKI en base64url) como objeto, comprobando que sea del tipo que dice `alg`."""
    if alg not in ALGORITMOS:
        raise PasskeyError("ALGORITMO", "Este dispositivo usa un tipo de llave que ILUS no admite.")
    try:
        clave = serialization.load_der_public_key(b64u_dec(publica_b64))
    except PasskeyError:
        raise
    except Exception:
        raise PasskeyError("CLAVE", "La llave del dispositivo no es válida. Intenta de nuevo.")
    ok = ((alg == ALG_ES256 and isinstance(clave, ec.EllipticCurvePublicKey) and clave.curve.name == "secp256r1")
          or (alg == ALG_RS256 and isinstance(clave, rsa.RSAPublicKey) and clave.key_size >= 2048)
          or (alg == ALG_EDDSA and isinstance(clave, ed25519.Ed25519PublicKey)))
    if not ok:
        raise PasskeyError("CLAVE", "La llave del dispositivo no es del tipo esperado. Intenta de nuevo.")
    return clave


def _verificar_firma(clave, alg, firma, datos):
    try:
        if alg == ALG_ES256:
            clave.verify(firma, datos, ec.ECDSA(hashes.SHA256()))
        elif alg == ALG_RS256:
            clave.verify(firma, datos, padding.PKCS1v15(), hashes.SHA256())
        else:
            clave.verify(firma, datos)
    except InvalidSignature:
        raise PasskeyError("FIRMA", "No se pudo comprobar que eres tú. Entra con tu contraseña.")


# ── Registro ────────────────────────────────────────────────────────────────
def verificar_registro(*, client_data_b64, auth_data_b64, publica_b64, alg, id_b64, desafio_b64, origenes, rp_id):
    """Comprueba el registro de un dispositivo. → {credential_id, publica, alg, contador} listo para guardar."""
    _client_data(client_data_b64, "webauthn.create", desafio_b64, origenes)
    _auth, ad = _auth_data(auth_data_b64, rp_id, exigir_at=True)
    credencial = b64u_dec(id_b64)
    if not credencial or len(credencial) > 1023 or not secrets.compare_digest(ad["credencial"], credencial):
        raise PasskeyError("DATOS", "La credencial no coincide con la que entregó el dispositivo. Intenta de nuevo.")
    cargar_clave(publica_b64, alg)
    return {"credential_id": b64u_enc(credencial), "publica": publica_b64, "alg": int(alg), "contador": ad["contador"]}


# ── Ingreso ─────────────────────────────────────────────────────────────────
def verificar_ingreso(*, client_data_b64, auth_data_b64, firma_b64, desafio_b64, origenes, rp_id, publica_b64, alg, contador_guardado):
    """Comprueba que quien entra firmó el desafío con la llave guardada y verificó su identidad en el teléfono. → el contador nuevo."""
    crudo = _client_data(client_data_b64, "webauthn.get", desafio_b64, origenes)
    auth, ad = _auth_data(auth_data_b64, rp_id)
    clave = cargar_clave(publica_b64, alg)
    _verificar_firma(clave, int(alg), b64u_dec(firma_b64), auth + hashlib.sha256(crudo).digest())
    nuevo, previo = ad["contador"], int(contador_guardado or 0)
    # Un contador que no avanza delata una copia de la llave. Los teléfonos que no lo usan (iCloud, Google) mandan siempre 0: eso es normal.
    if (nuevo or previo) and nuevo <= previo:
        raise PasskeyError("CLON", "Esta verificación parece repetida. Entra con tu contraseña y vuelve a activar Face ID.")
    return nuevo


# ── Origen y dominio de esta instalación ────────────────────────────────────
def rp_id_de(base_url, forzado=None):
    """El dominio con el que se atan las credenciales (sin puerto): `https://ilus-app-….run.app` → `ilus-app-….run.app`."""
    if forzado:
        return str(forzado).strip().lower()
    host = re.sub(r"^https?://", "", str(base_url or "")).split("/")[0].split(":")[0]
    return host.lower()


def origenes_permitidos(base_url, extra=""):
    lista = {str(base_url or "").rstrip("/")}
    for o in str(extra or "").split(","):
        o = o.strip().rstrip("/")
        if o.startswith("https://") or o.startswith("http://localhost"):
            lista.add(o)
    lista.discard("")
    return lista

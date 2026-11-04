# -*- coding: utf-8 -*-
"""🔐 PASSKEYS (Face ID / huella) · rutas y base de datos — Daniel, 05-nov-2026: «que desbloquee la aplicación con la huella o los métodos de seguridad
que tenga el teléfono… que le pregunte si quiere y dejarlo habilitado por el front… allí, donde salen mis datos (Mi cuenta), necesito controlarlo».

ES UN MODO ADICIONAL DE ENTRAR: la contraseña sigue igual y sigue siendo el respaldo. Nadie queda sin poder entrar por esto.

  · Cada persona activa Face ID / huella DISPOSITIVO POR DISPOSITIVO desde Mi cuenta (o aceptando la pregunta que sale en el celular) y lo apaga igual.
  · ILUS guarda solo la llave PÚBLICA; la privada nunca sale del teléfono y solo se usa tras Face ID, huella o el código del teléfono.
  · El ingreso crea la MISMA sesión que la contraseña (mismo tiempo de inactividad, misma auditoría con método «passkey»).
  · Un usuario desactivado no entra, aunque conserve su llave. Quitar un dispositivo (o que lo quite la gestión) corta ese acceso al instante.
  · Interruptor: ILUS_PASSKEYS=0 lo apaga en todo el sistema (nada se muestra, las rutas responden 404).
Las reglas de verificación viven en passkeys_reglas.py (puras y probadas con firmas reales).
"""
import os
import re

from flask import g, jsonify, redirect, request, session

import passkeys_reglas as pk

_MSG_FALLA_INGRESO = "No se pudo comprobar tu Face ID o huella. Entra con tu contraseña."


def activas():
    """Interruptor general: por defecto encendido; ILUS_PASSKEYS=0 (o false/no/off) lo apaga."""
    return str(os.environ.get("ILUS_PASSKEYS", "1")).strip().lower() not in ("0", "false", "no", "off", "")


def register_passkey_routes(app, ctx):
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    mysql_rowcount = ctx["mysql_execute_returning_rowcount"]
    login_required = ctx["login_required"]
    get_user = ctx["get_auth_user_by_id"]
    audit = ctx.get("_audit") or (lambda *a, **k: None)
    rate_limited = ctx.get("rate_limited") or (lambda scope, n, s, methods=("POST",): (lambda v: v))
    base_url = ctx.get("_public_base_url") or (lambda: request.url_root.rstrip("/"))
    auth_table = str(ctx.get("AUTH_TABLE") or "app_users")

    # ── Tabla (una sola sentencia, barata si ya existe) ──────────────────────
    def _ensure_tabla():
        try:
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS user_passkeys (
                  id             INT AUTO_INCREMENT PRIMARY KEY,
                  user_id        INT          NOT NULL,
                  credential_id  VARCHAR(1400) NOT NULL,
                  public_key     TEXT         NOT NULL,
                  alg            INT          NOT NULL,
                  sign_count     BIGINT       NOT NULL DEFAULT 0,
                  transports     VARCHAR(120) NULL,
                  etiqueta       VARCHAR(120) NULL,
                  user_agent     VARCHAR(255) NULL,
                  creada_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                  ultimo_uso_at  DATETIME NULL,
                  revocada_at    DATETIME NULL,
                  revocada_por   VARCHAR(190) NULL,
                  UNIQUE KEY uq_passkey_cred (credential_id(255)),
                  KEY idx_passkey_user (user_id, revocada_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        except Exception as e:
            print(f"[ILUS][WARN] _ensure_passkeys: {type(e).__name__}: {e}", flush=True)

    try:
        with app.app_context():
            _ensure_tabla()
    except Exception as e:
        print(f"[ILUS][WARN] passkeys (tabla): {type(e).__name__}: {e}", flush=True)

    # ── Utilidades ───────────────────────────────────────────────────────────
    def _ok(**extra):
        return jsonify({"ok": True, **extra})

    def _err(msg, status=400, codigo=None):
        return jsonify({"ok": False, "error": msg, **({"error_codigo": codigo} if codigo else {})}), status

    def _rp_id():
        return pk.rp_id_de(base_url(), os.environ.get("ILUS_PASSKEY_RP_ID"))

    def _origenes():
        return pk.origenes_permitidos(base_url(), os.environ.get("ILUS_PASSKEY_ORIGENES"))

    def _fmt(dt):
        try:
            return dt.strftime("%d/%m/%Y %H:%M") if dt else ""
        except Exception:
            return ""

    def _siguiente(valor):
        """`next` solo puede ser una ruta de ESTA app (mismo criterio que /login)."""
        v = valor if isinstance(valor, str) else ""
        if not v.startswith("/") or v.startswith("//") or v.startswith("/\\"):
            return "/"
        return v

    def _etiqueta(dato):
        t = re.sub(r"\s+", " ", str(dato or "")).strip()[:120]
        return t or "Mi dispositivo"

    @app.context_processor
    def _contexto_passkeys():
        return {"passkeys_activas": activas()}

    def _filas_de(uid):
        return mysql_fetchall(
            "SELECT id, credential_id, etiqueta, user_agent, creada_at, ultimo_uso_at FROM user_passkeys "
            "WHERE user_id=%s AND revocada_at IS NULL ORDER BY id", (uid,)) or []

    def _json_fila(f):
        return {"id": f["id"], "credencial": f.get("credential_id"), "etiqueta": f.get("etiqueta") or "Mi dispositivo", "creada_fmt": _fmt(f.get("creada_at")),
                "ultimo_uso_fmt": _fmt(f.get("ultimo_uso_at")) or "Todavía no se ha usado"}

    # ── Mis dispositivos (Mi cuenta) ─────────────────────────────────────────
    @app.route("/passkeys/mis", methods=["GET"])
    @login_required
    def passkeys_mis():
        if not activas():
            return _err("Face ID no está disponible.", 404)
        try:
            filas = _filas_de(g.user["id"])
        except Exception as e:
            print(f"[passkeys] listar: {type(e).__name__}: {e}", flush=True)
            return _err("No se pudo leer tus dispositivos. Intenta de nuevo.", 500)
        return _ok(dispositivos=[_json_fila(f) for f in filas], maximo=pk.MAX_POR_USUARIO)

    # ── Registro de un dispositivo ───────────────────────────────────────────
    @app.route("/passkeys/registro/opciones", methods=["POST"])
    @login_required
    @rate_limited("passkeys_reg_opc", 20, 600)
    def passkeys_registro_opciones():
        if not activas():
            return _err("Face ID no está disponible.", 404)
        u = g.user
        try:
            existentes = [f["credential_id"] for f in (mysql_fetchall(
                "SELECT credential_id FROM user_passkeys WHERE user_id=%s AND revocada_at IS NULL", (u["id"],)) or [])]
        except Exception as e:
            print(f"[passkeys] opciones de registro: {type(e).__name__}: {e}", flush=True)
            return _err("No se pudo preparar la activación. Intenta de nuevo.", 500)
        if len(existentes) >= pk.MAX_POR_USUARIO:
            return _err(f"Ya tienes {pk.MAX_POR_USUARIO} dispositivos con Face ID. Quita alguno para agregar otro.", 409, "LIMITE")
        des = pk.nuevo_desafio("registro")
        session["pk_desafio"] = des
        nombre = (u.get("nombre") or u.get("username") or "").strip()
        return _ok(publicKey={
            "challenge": des["c"], "rp": {"id": _rp_id(), "name": "ILUS Fitness"},
            "user": {"id": pk.b64u_enc(str(u["id"]).encode("ascii")), "name": u.get("username") or str(u["id"]), "displayName": nombre or u.get("username") or "Usuario"},
            "pubKeyCredParams": [{"type": "public-key", "alg": a} for a in pk.ALGORITMOS],
            "authenticatorSelection": {"authenticatorAttachment": "platform", "userVerification": "required", "residentKey": "preferred"},
            "attestation": "none", "timeout": 60000,
            "excludeCredentials": [{"type": "public-key", "id": c} for c in existentes]})

    @app.route("/passkeys/registro/verificar", methods=["POST"])
    @login_required
    @rate_limited("passkeys_reg_ver", 20, 600)
    def passkeys_registro_verificar():
        if not activas():
            return _err("Face ID no está disponible.", 404)
        d = request.get_json(silent=True) or {}
        u = g.user
        try:
            desafio = pk.comprobar_desafio(session.pop("pk_desafio", None), "registro")
            r = pk.verificar_registro(
                client_data_b64=d.get("clientDataJSON"), auth_data_b64=d.get("authenticatorData"), publica_b64=d.get("publicKey"),
                alg=int(d.get("publicKeyAlgorithm") or 0), id_b64=d.get("id"), desafio_b64=desafio, origenes=_origenes(), rp_id=_rp_id())
        except (pk.PasskeyError, TypeError, ValueError) as e:
            audit("passkey_registro_fallo", target_type="user", target_id=u["id"], details={"motivo": getattr(e, "codigo", "DATOS")}, status="fail")
            return _err(getattr(e, "mensaje", "Los datos del dispositivo no se entendieron. Intenta de nuevo."), 400, getattr(e, "codigo", "DATOS"))
        try:
            n = int((mysql_fetchone("SELECT COUNT(*) AS n FROM user_passkeys WHERE user_id=%s AND revocada_at IS NULL", (u["id"],)) or {}).get("n") or 0)
            if n >= pk.MAX_POR_USUARIO:
                return _err(f"Ya tienes {pk.MAX_POR_USUARIO} dispositivos con Face ID. Quita alguno para agregar otro.", 409, "LIMITE")
            mysql_execute(
                "INSERT INTO user_passkeys (user_id, credential_id, public_key, alg, sign_count, transports, etiqueta, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (u["id"], r["credential_id"], r["publica"], r["alg"], r["contador"],
                 ",".join(str(x)[:20] for x in (d.get("transports") or [])[:6]) or None,
                 _etiqueta(d.get("etiqueta")), (request.headers.get("User-Agent") or "")[:255]))
        except Exception as e:
            if "1062" in str(e) or "uplicate" in str(e):
                return _err("Este dispositivo ya estaba activado.", 409, "DUPLICADO")
            print(f"[passkeys] guardar: {type(e).__name__}: {e}", flush=True)
            return _err("No se pudo guardar. Intenta de nuevo.", 500)
        audit("passkey_registro", target_type="user", target_id=u["id"], details={"etiqueta": _etiqueta(d.get("etiqueta"))})
        return _ok(dispositivos=[_json_fila(f) for f in _filas_de(u["id"])], credencial=r["credential_id"])

    # ── Quitar un dispositivo (el dueño, o un superadministrador) ────────────
    @app.route("/passkeys/<int:pid>/quitar", methods=["POST"])
    @login_required
    def passkeys_quitar(pid):
        if not activas():
            return _err("Face ID no está disponible.", 404)
        fila = mysql_fetchone("SELECT id, user_id FROM user_passkeys WHERE id=%s AND revocada_at IS NULL", (pid,))
        if not fila:
            return _err("No encontré ese dispositivo.", 404)
        u = g.user
        if fila["user_id"] != u["id"] and (u.get("role") or "") != "superadmin":
            return _err("No puedes quitar el dispositivo de otra persona.", 403, "SIN_PERMISO")
        n = mysql_rowcount("UPDATE user_passkeys SET revocada_at=NOW(), revocada_por=%s WHERE id=%s AND revocada_at IS NULL", (u.get("username"), pid))
        if not n:
            return _err("Ese dispositivo ya estaba quitado.", 409)
        audit("passkey_quitada", target_type="user", target_id=fila["user_id"], details={"passkey": pid, "por": u.get("username")})
        return _ok(dispositivos=[_json_fila(f) for f in _filas_de(u["id"])])

    # ── Ingreso con Face ID / huella (sin sesión: por eso lleva límite de intentos) ──
    @app.route("/passkeys/login/opciones", methods=["POST"])
    @rate_limited("passkeys_ing_opc", 40, 300)
    def passkeys_login_opciones():
        if not activas():
            return _err("Face ID no está disponible.", 404)
        des = pk.nuevo_desafio("login")
        session["pk_desafio"] = des
        return _ok(publicKey={"challenge": des["c"], "rpId": _rp_id(), "timeout": 60000, "userVerification": "required"})

    @app.route("/passkeys/login/verificar", methods=["POST"])
    @rate_limited("passkeys_ing_ver", 12, 300)
    def passkeys_login_verificar():
        if not activas():
            return _err("Face ID no está disponible.", 404)
        d = request.get_json(silent=True) or {}
        desafio_guardado = session.pop("pk_desafio", None)

        def _falla(motivo, uid=None, msg=_MSG_FALLA_INGRESO, status=401):
            audit("login_fail", target_type="user", target_id=uid, details={"metodo": "passkey", "reason": motivo}, status="fail")
            return _err(msg, status, motivo)

        try:
            cred = pk.b64u_enc(pk.b64u_dec(d.get("id")))
        except pk.PasskeyError:
            return _falla("DATOS")
        try:
            fila = mysql_fetchone("SELECT * FROM user_passkeys WHERE credential_id=%s AND revocada_at IS NULL", (cred,))
        except Exception as e:
            print(f"[passkeys] buscar credencial: {type(e).__name__}: {e}", flush=True)
            return _err("No fue posible conectar con el servidor. Intenta nuevamente en unos segundos.", 503)
        if not fila:
            return _falla("DESCONOCIDA", msg="Este dispositivo no tiene Face ID activado en ILUS. Entra con tu contraseña y actívalo en Mi cuenta.")
        try:
            desafio = pk.comprobar_desafio(desafio_guardado, "login")
            handle = d.get("userHandle")
            if handle:
                if pk.b64u_dec(handle) != str(fila["user_id"]).encode("ascii"):
                    raise pk.PasskeyError("USUARIO", _MSG_FALLA_INGRESO)
            nuevo = pk.verificar_ingreso(
                client_data_b64=d.get("clientDataJSON"), auth_data_b64=d.get("authenticatorData"), firma_b64=d.get("signature"), desafio_b64=desafio,
                origenes=_origenes(), rp_id=_rp_id(), publica_b64=fila["public_key"], alg=fila["alg"], contador_guardado=fila.get("sign_count"))
        except pk.PasskeyError as e:
            return _falla(e.codigo, fila["user_id"], e.mensaje)
        user = get_user(fila["user_id"])
        if not user or not user.get("active"):
            return _falla("INACTIVO", fila["user_id"])
        try:
            mysql_execute("UPDATE user_passkeys SET sign_count=GREATEST(sign_count, %s), ultimo_uso_at=NOW() WHERE id=%s", (nuevo, fila["id"]))
        except Exception as e:
            print(f"[passkeys] contador: {type(e).__name__}: {e}", flush=True)
        # La MISMA sesión que da la contraseña (misma inactividad de 60 min).
        session.clear()
        session["user_id"] = user["id"]
        session.permanent = True
        try:
            ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "")[:64]
            mysql_execute(f"UPDATE `{auth_table}` SET last_login_at=NOW(), last_login_ip=%s, last_seen_at=NOW(), login_count=COALESCE(login_count,0)+1 WHERE id=%s",
                          (ip, user["id"]))
        except Exception as e:
            print(f"[passkeys] seguimiento del ingreso: {type(e).__name__}: {e}", flush=True)
        audit("login_ok", target_type="user", target_id=user["id"],
              details={"metodo": "passkey", "ua": (request.headers.get("User-Agent") or "")[:180]},
              user_override={"id": user["id"], "username": user["username"], "role": user["role"]})
        return _ok(redirect=_siguiente(d.get("next")), nombre=user.get("nombre") or "")

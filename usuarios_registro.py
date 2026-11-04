# -*- coding: utf-8 -*-
"""Usuario nuevo «en espera» y su registro (Daniel, 2026-11-04).

Pedido: «cuando vaya a crear un usuario nuevo este deberá estar stand by si no se registra, y para crear un usuario nuevo solo necesito
su correo y su cargo; él debe rellenar su RUT, nombre y apellido, su dirección, RUT y teléfono con formato chileno».

Cómo queda:
  · Quien administra crea el usuario con SOLO correo y cargo (el cargo es el rol del sistema). El usuario nace «en espera»: no puede entrar
    ni aparece para asignarle trabajo hasta que complete su registro desde el enlace que recibe por correo (/welcome/<token>).
  · «En espera» no necesita una columna nueva (REGLA #18): es `active=0` + `requires_password_change=1` + nunca inició sesión. Esas
    tres cosas ya existen y ya significan «invitado que todavía no activó su cuenta».
  · Al registrarse (nombre, apellido, RUT, teléfono, dirección validada con Google y su propia clave) la cuenta se activa sola.

Módulo PURO (sin Flask ni base de datos) para probarlo rápido. El RUT se valida con validar_rut() de app.py (módulo 11, estricto) y la
dirección con usuarios_perfil.resolver_direccion; aquí viven las reglas de nombre, teléfono y de «en espera».
"""
import re

_LETRAS = "A-Za-zÁÉÍÓÚÜÑáéíóúüñ"
_PARTE_RE = re.compile(r"^[%s][%s'’\- ]{0,58}[%s]$" % (_LETRAS, _LETRAS, _LETRAS))
_TEL_RE = re.compile(r"^[2-9][0-9]{8}$")

MSG_TEL = ("El teléfono debe ser chileno: 9 dígitos, por ejemplo 9 1234 5678 (celular) o 2 2345 6789 (fijo). "
           "El +56 se agrega solo.")


def _a_texto(v):
    return re.sub(r"\s+", " ", str(v if v is not None else "")).strip()


def normalizar_parte(txt):
    """Colapsa espacios y, si lo escribieron todo en minúsculas o todo en mayúsculas, lo deja con inicial mayúscula (maría josé → María José)."""
    t = _a_texto(txt)
    if t and (t == t.lower() or t == t.upper()):
        t = t.title()
    return t


def validar_parte_nombre(txt, etiqueta="El nombre"):
    """Un nombre o un apellido: solo letras (con tildes y ñ), espacios, guion y apóstrofe; entre 2 y 60 caracteres.
    Devuelve (True, valor) o (False, mensaje)."""
    t = normalizar_parte(txt)
    if not t:
        return False, f"{etiqueta} es obligatorio."
    if not _PARTE_RE.match(t):
        return False, f"{etiqueta} solo puede llevar letras (mínimo 2), sin números ni símbolos."
    return True, t


def armar_nombre(nombre, apellido):
    """«Nombre Apellido» tal como se guarda en app_users.nombre (una sola columna)."""
    return f"{normalizar_parte(nombre)} {normalizar_parte(apellido)}".strip()


def separar_nombre(nombre_completo):
    """Para precargar el formulario: lo primero es el nombre y el resto el apellido. («Ana María Soto Pérez» → «Ana», «María Soto Pérez»;
    la persona lo corrige si hace falta.)"""
    t = _a_texto(nombre_completo)
    if not t:
        return "", ""
    partes = t.split(" ", 1)
    return partes[0], (partes[1] if len(partes) > 1 else "")


def telefono_cl(raw):
    """Teléfono chileno ESTRICTO: 9 dígitos (celular 9XXXXXXXX o fijo con código de área), con o sin +56. Devuelve (True, '+56912345678')
    o (False, mensaje). Lo vacío es error: en el registro el teléfono es obligatorio."""
    d = "".join(c for c in str(raw or "") if c.isdigit())
    if d.startswith("56") and len(d) >= 11:
        d = d[2:]
    if not _TEL_RE.match(d):
        return False, MSG_TEL
    return True, "+56" + d


def telefono_para_formulario(guardado):
    """'+56912345678' → '9 1234 5678' (como lo escribe la persona; el +56 va fijo al lado del campo)."""
    d = "".join(c for c in str(guardado or "") if c.isdigit())
    if d.startswith("56") and len(d) >= 11:
        d = d[2:]
    if len(d) == 9:
        return f"{d[0]} {d[1:5]} {d[5:]}"
    return d


def nombre_provisional(correo):
    """Lo que se guarda en `nombre` mientras la persona no se registra (la columna no admite vacío): lo anterior a la @ con cara de nombre."""
    local = _a_texto(str(correo or "").split("@")[0]).replace(".", " ").replace("_", " ").replace("-", " ")
    local = re.sub(r"[0-9]+", "", local).strip()
    return (local.title() if local else "Usuario nuevo")[:60]


def nombre_es_provisional(nombre, correo):
    """¿El nombre guardado es el provisional (la persona todavía no puso el suyo)?"""
    n = _a_texto(nombre).lower()
    return (not n) or n == nombre_provisional(correo).lower()


def esta_en_espera(fila):
    """¿Invitado que todavía no completó su registro? active=0 + requires_password_change=1 + nunca inició sesión.
    `fila` es un dict con esas columnas de app_users (las que falten cuentan como «no en espera»)."""
    if not fila:
        return False
    activo = fila.get("active")
    pide_clave = fila.get("requires_password_change")
    return (activo in (0, False, "0", None) and activo is not None
            and pide_clave in (1, True, "1")
            and not fila.get("last_login_at"))

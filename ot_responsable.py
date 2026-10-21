# -*- coding: utf-8 -*-
"""🙋 Responsable de la OT (F0, REGLA #36) — reglas PURAS: sin base de datos, sin Flask, sin correo.

Daniel (20-oct-2026): «¿los avisos le llegan a la persona que administra las órdenes de trabajo?» → cada OT tiene UN responsable
(Víctor, Aarón o Juan Pablo). Lo que se decidió con él, y lo que este módulo deja escrito en un solo lugar:

  1. El responsable es un PERMISO («Responsable de OT», Roles → Servicio Técnico), apagado para todos y encendido solo para esas
     tres personas. Candidato = usuario ACTIVO con el permiso EXPLÍCITO (por rol o por persona). El superadministrador NO entra por
     tener «todo» (Daniel no es responsable de nada por serlo) y la familia técnico nunca (REGLA #19/#26: no ve plata ni proveedores).
  2. Al crear: si quien la crea es candidato, queda él; si no, el ejecutivo del cliente (si también es candidato); si tampoco hay, el
     asistente PIDE elegirlo (REGLA #32: el sistema sugiere, la persona decide). Sin candidatos (el permiso aún no se enciende) nada
     se pide ni se bloquea: la OT queda sin responsable y los avisos siguen yendo como hoy.
  3. Cambiarlo: solo gestión (superadmin, admin, supervisor) y el propio responsable actual; con motivo opcional; queda en la
     bitácora (quién, cuándo, de quién a quién) y se avisa por la campana, con tono amable, a la persona nueva y a la anterior.

Los avisos de este módulo son INTERNOS (campana): al cliente no le llega nada (REGLA #22).
"""
import re

# ───────────────────────── constantes ─────────────────────────
PERMISO_KEY = "mant_ot_responsable"            # llave de g.permissions y de usuario_permisos (permisos individuales)
PERMISO_MODULO = "mantenciones"                # fila de rol_permisos: (rol_slug, 'mantenciones', 'ot_responsable')
PERMISO_ACCION = "ot_responsable"
FAMILIAS_QUE_CAMBIAN = ("superadmin", "admin", "supervisor")     # además del propio responsable actual
FAMILIA_TECNICO = "tecnico"
ESTADOS_TERMINALES = ("cerrada", "cancelada", "anulada")         # solo el superadministrador mueve el responsable de una OT terminada

ORIGEN_CREADOR = "creador"
ORIGEN_EJECUTIVO = "ejecutivo"
ORIGEN_ELEGIDO = "elegido"
ORIGEN_HEREDADO = "heredado"
ORIGEN_CAMBIO = "cambio"

TIPO_AVISO = "ot_responsable"                  # valor del ENUM de mant_notificaciones.tipo (va AL FINAL de la lista)
MOTIVO_MAX = 500
LINEA_MAX = 190                                # mant_visitas.responsable_nombre / responsable_por son VARCHAR(190)

_ESPACIOS = re.compile(r"\s+")


def entero(x):
    """int > 0 o None (acepta '7', 7.0, ' 7 '; rechaza '', None, 0, 'x', True)."""
    if isinstance(x, bool):
        return None
    try:
        n = int(float(str(x).strip()))
    except (TypeError, ValueError, OverflowError):
        return None
    return n if n > 0 else None


_entero = entero      # alias interno (el resto del módulo y sus pruebas lo usan con este nombre)


def limpiar(texto, largo=LINEA_MAX):
    """Una línea sin saltos ni espacios de más y con tope de largo."""
    return _ESPACIOS.sub(" ", str(texto or "")).strip()[:largo]


# ───────────────────────── quién puede ser responsable ─────────────────────────
def tiene_permiso(permiso_persona, permiso_rol):
    """El permiso explícito: la fila de la PERSONA (usuario_permisos) manda sobre la de su ROL (rol_permisos); sin ninguna fila es
    False (nace apagado para todos). NO existe el atajo de superadministrador: ver el docstring del módulo."""
    if permiso_persona is not None:
        return bool(permiso_persona)
    return bool(permiso_rol)


def es_candidato(familia, activo, permiso_persona, permiso_rol):
    """¿Puede ser el responsable de una OT? Activo, con el permiso explícito y fuera de la familia técnico."""
    if not activo:
        return False
    if (familia or "") == FAMILIA_TECNICO:
        return False
    return tiene_permiso(permiso_persona, permiso_rol)


def candidatos_de_filas(filas, familia_de):
    """[{id, nombre, rol}] ordenados por nombre, a partir de filas {id, username, nombre, role, ov, rolp} de app_users (activos).
    `familia_de(role)` es _rol_familia. Sin repetidos."""
    out, vistos = [], set()
    for f in (filas or []):
        uid = _entero(f.get("id"))
        if not uid or uid in vistos:
            continue
        if not es_candidato(familia_de(f.get("role")), True, f.get("ov"), f.get("rolp")):
            continue
        vistos.add(uid)
        nombre = limpiar(f.get("nombre") or f.get("username") or ("Usuario %d" % uid))
        out.append({"id": uid, "nombre": nombre, "rol": limpiar(f.get("role"), 60)})
    out.sort(key=lambda c: (c["nombre"].lower(), c["id"]))
    return out


# ───────────────────────── el defecto al crear ─────────────────────────
def decidir_defecto(ids_candidatos, creador_id, ejecutivo_id):
    """(user_id, origen) del responsable por defecto, o (None, None).
    Quien crea la OT si es candidato; si no, el ejecutivo del cliente si es candidato; si no, nadie (el asistente lo pide)."""
    cands = {c for c in (_entero(i) for i in (ids_candidatos or [])) if c}
    cre = _entero(creador_id)
    if cre and cre in cands:
        return cre, ORIGEN_CREADOR
    eje = _entero(ejecutivo_id)
    if eje and eje in cands:
        return eje, ORIGEN_EJECUTIVO
    return None, None


def validar_elegido(elegido_id, ids_candidatos):
    """El id que eligió la persona si de verdad es candidato; si no, None (lo elegido se revalida SIEMPRE en el servidor)."""
    uid = _entero(elegido_id)
    cands = {c for c in (_entero(i) for i in (ids_candidatos or [])) if c}
    return uid if (uid and uid in cands) else None


def texto_origen(origen, quien=None, madre=None):
    """Por qué esa persona quedó como responsable, para la bitácora y para el asistente («Sugerido: …»)."""
    if origen == ORIGEN_CREADOR:
        return "es quien creó la OT"
    if origen == ORIGEN_EJECUTIVO:
        return "es el ejecutivo del cliente"
    if origen == ORIGEN_HEREDADO:
        return "hereda el responsable de " + (limpiar(madre, 60) or "la OT de origen")
    if origen == ORIGEN_ELEGIDO:
        return "la eligió %s al crear la OT" % (limpiar(quien, 80) or "quien la creó")
    return ""


# ───────────────────────── cambiar el responsable ─────────────────────────
def puede_cambiar(familia, user_id, responsable_actual_id):
    """Gestión (superadmin, admin, supervisor) o el propio responsable actual. Un técnico, nunca."""
    if (familia or "") == FAMILIA_TECNICO:
        return False
    if (familia or "") in FAMILIAS_QUE_CAMBIAN:
        return True
    uid, act = _entero(user_id), _entero(responsable_actual_id)
    return bool(uid and act and uid == act)


def normalizar_motivo(motivo):
    """Motivo opcional: una sola línea, hasta MOTIVO_MAX. '' si no hay."""
    return limpiar(motivo, MOTIVO_MAX)


def validar_cambio(actual_id, nuevo_id, ids_candidatos, estado_ot, es_superadmin):
    """(ok, codigo, mensaje). El nuevo debe ser un candidato activo y distinto del actual; una OT terminada solo la mueve el
    superadministrador. Mensajes para la pantalla, en tono amable."""
    nuevo = _entero(nuevo_id)
    if not nuevo:
        return False, "RESPONSABLE_REQUERIDO", "Elige quién queda a cargo de la OT."
    cands = {c for c in (_entero(i) for i in (ids_candidatos or [])) if c}
    if nuevo not in cands:
        return False, "RESPONSABLE_NO_VALIDO", ("Esa persona no puede ser responsable de una OT: necesita el permiso «Responsable de OT» "
                                                "(se enciende en Roles).")
    if _entero(actual_id) == nuevo:
        return False, "YA_ES_RESPONSABLE", "Esa persona ya es la responsable de esta OT."
    if (estado_ot or "").strip().lower() in ESTADOS_TERMINALES and not es_superadmin:
        return False, "OT_TERMINADA", "La OT ya terminó: su responsable queda como estaba. Si hace falta cambiarlo, avisa al superadministrador."
    return True, None, ""


# ───────────────────────── textos (bitácora y campana) ─────────────────────────
def detalle_bitacora_asignado(nombre, origen, quien=None, madre=None):
    """Bitácora de la asignación al crear la OT: «Responsable: Aarón Rojas · es quien creó la OT»."""
    por = texto_origen(origen, quien, madre)
    return limpiar("Responsable: %s%s" % (limpiar(nombre, 120) or "—", (" · " + por) if por else ""), 600)


def detalle_bitacora_cambio(anterior, nuevo, quien, motivo=""):
    """Bitácora del cambio: de quién a quién, quién lo hizo y el motivo si lo hubo. La hora la pone la bitácora (hora Chile)."""
    ant = limpiar(anterior, 120) or "sin responsable"
    txt = "Responsable: %s → %s · por %s" % (ant, limpiar(nuevo, 120) or "—", limpiar(quien, 120) or "el sistema")
    m = normalizar_motivo(motivo)
    if m:
        txt += " · Motivo: " + m
    return limpiar(txt, 900)


def titulo_aviso_nuevo(numero_ot):
    return limpiar("Ahora estás a cargo de %s" % (numero_ot or "una orden de trabajo"), 190)


def _donde(numero_ot, cliente):
    """«OT-2026-00123 · Gimnasio X» (o solo la OT si no hay cliente)."""
    ot = limpiar(numero_ot, 60) or "la orden de trabajo"
    cli = limpiar(cliente, 120)
    return ot + ((" · " + cli) if cli else "")


def cuerpo_aviso_nuevo(numero_ot, cliente, quien, origen=None, motivo="", anterior=None):
    """Para la persona que queda a cargo. Amable y con el contexto; nunca plata."""
    donde = _donde(numero_ot, cliente)
    q = limpiar(quien, 80)
    if origen == ORIGEN_EJECUTIVO:
        base = "Quedaste como responsable de %s porque eres el ejecutivo del cliente." % donde
    elif origen == ORIGEN_ELEGIDO:
        base = "%s te eligió como responsable al crear %s." % (q or "Quien la creó", donde)
    else:
        base = "%s dejó %s a tu cargo." % (q or "El equipo", donde)
    extra = []
    ant = limpiar(anterior, 80)
    if ant:
        extra.append("La llevaba %s." % ant)
    m = normalizar_motivo(motivo)
    if m:
        extra.append("Motivo: %s." % m.rstrip("."))
    extra.append("Puedes abrirla desde aquí.")
    return limpiar(base + " " + " ".join(extra), 1500)


def titulo_aviso_anterior(numero_ot, nuevo):
    return limpiar("%s pasó a cargo de %s" % (numero_ot or "La orden de trabajo", limpiar(nuevo, 80) or "otra persona"), 190)


def cuerpo_aviso_anterior(numero_ot, cliente, quien, nuevo, motivo=""):
    """Para quien la llevaba hasta ahora: sin nada que hacer y sin nada que suene a castigo."""
    base = "%s dejó a %s como responsable de %s." % (limpiar(quien, 80) or "El equipo", limpiar(nuevo, 80) or "otra persona",
                                                      _donde(numero_ot, cliente))
    extra = []
    m = normalizar_motivo(motivo)
    if m:
        extra.append("Motivo: %s." % m.rstrip("."))
    extra.append("Queda en buenas manos: no necesitas hacer nada más.")
    return limpiar(base + " " + " ".join(extra), 1500)


# Palabras que NO deben aparecer en un aviso de cambio (suenan a castigo o a reclamo). Las vigila una prueba.
PALABRAS_PROHIBIDAS_EN_AVISOS = ("quitamos", "te quitaron", "quitó", "removido", "removimos", "sacamos", "te sacaron", "perdiste",
                                  "reemplazamos", "reemplazado", "despojado", "ya no eres", "dejaste de ser", "culpa", "falla tuya")


# ───────────────────────── filtro de la tabla de OT ─────────────────────────
def normalizar_filtro(valor, uid_actual):
    """('', None) sin filtro · ('mis', uid) · ('sin', None) · ('id', n). Lista blanca: cualquier otra cosa no filtra."""
    v = str(valor or "").strip().lower()
    if v == "mis":
        n = _entero(uid_actual)
        return ("mis", n) if n else ("", None)
    if v == "sin":
        return "sin", None
    n = _entero(v) if v.isdigit() else None
    return ("id", n) if n else ("", None)

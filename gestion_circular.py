# -*- coding: utf-8 -*-
"""GESTIÓN CIRCULAR de hallazgos e inquietudes de clientes (Daniel, 26-oct-2026).

Daniel: «necesito un CRUD en los hallazgos… borrar algunos hallazgos que hemos hecho de prueba… solamente para el superadministrador…
tomar el levantamiento… editarla, crear un repuesto, solicitarlo, generar una OT, generar un ticket… gestionando de manera circular»; y
«agreguemos todo para que no se quede nada atrás: la atención de hallazgos y la gestión de tickets, OT, solicitud y gestión de repuestos,
además de su valorización».

Este módulo es PURO (sin base de datos, sin Flask): las validaciones y los textos que usan app.py, las plantillas y las pruebas.
  · Editar un hallazgo: título, detalle y gravedad, con MOTIVO (queda en su registro de actividad y en la bitácora de la OT).
  · Borrar (solo superadministrador): escribiendo BORRAR y un motivo; antes de borrar se guarda la FOTO completa de la fila (REGLA #5)
    y una «lápida» para que el mismo hallazgo no vuelva a nacer solo cuando el técnico vuelva a guardar esa tarea.
  · Valorizar: lo que se estima que cuesta resolverlo (repuestos + servicio), con una nota. Es plata: solo la gestión la ve; a un técnico
    nunca le viaja (REGLAS #19 y #26).
  · Los vínculos (ticket, solicitud de repuesto, OT de seguimiento) se muestran como un recorrido con semáforo (REGLA #15).
"""
import re

ENTIDAD_HALLAZGO = "hallazgo"
ENTIDAD_INQUIETUD = "inquietud"
ENTIDADES = (ENTIDAD_HALLAZGO, ENTIDAD_INQUIETUD)

MOTIVO_MIN = 10
MOTIVO_MAX = 400
TITULO_MAX = 300
DETALLE_MAX = 1500
NOTA_MAX = 300
VALOR_MAX = 100_000_000          # $100 millones por casilla: más es un error de tipeo
CONFIRMAR_BORRAR = "BORRAR"

SEVERIDADES_EDITABLES = ("falla", "alerta", "fuera_servicio", "baja", "repuesto", "incidencia")

# Lo que se lee en el registro de actividad de un hallazgo (la acción guardada → texto para la persona).
ACCIONES_TXT = {
    "editado": "Editó el hallazgo",
    "valorizado": "Valorizó",
    "ticket": "Levantó un ticket",
    "repuesto": "Solicitó repuesto",
    "tomado": "Tomó el caso",
    "descartado": "Descartó",
    "reabierto": "Reabrió",
    "seguimiento": "Levantó la OT de seguimiento",
}
ACCIONES_COLOR = {"editado": "azul", "valorizado": "verde", "ticket": "azul", "repuesto": "azul", "tomado": "azul",
                  "descartado": "gris", "reabierto": "ambar", "seguimiento": "verde"}


def limpio(s, tope=None):
    t = re.sub(r"\s+", " ", str(s or "")).strip()
    return t[:tope] if tope else t


def texto_largo(s, tope=DETALLE_MAX):
    """Como `limpio` pero respeta los saltos de línea (el detalle puede tener varias líneas)."""
    lineas = [re.sub(r"[ \t]+", " ", x).strip() for x in str(s or "").replace("\r\n", "\n").split("\n")]
    t = "\n".join(lineas).strip()
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t[:tope]


def entero(x):
    try:
        n = int(str(x).strip())
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def validar_motivo(motivo, que="el cambio"):
    """(motivo_limpio, None) o (None, error para la persona)."""
    m = limpio(motivo, MOTIVO_MAX)
    if len(m) < MOTIVO_MIN:
        return None, "Cuenta por qué se hace %s (mínimo %d caracteres): queda escrito con tu nombre." % (que, MOTIVO_MIN)
    return m, None


def validar_edicion(datos, actual):
    """Editar un hallazgo. `datos`: titulo, detalle, severidad, motivo. `actual`: la fila de hoy.
    → (cambios: dict solo con lo que cambia, motivo, None) o (None, None, error). Sin cambios es un error amable."""
    datos = datos or {}
    actual = actual or {}
    titulo = limpio(datos.get("titulo"), TITULO_MAX)
    if len(titulo) < 3:
        return None, None, "El título no puede quedar vacío."
    detalle = texto_largo(datos.get("detalle"))
    sev = str(datos.get("severidad") or actual.get("severidad") or "").strip()
    if sev not in SEVERIDADES_EDITABLES:
        return None, None, "Elige la gravedad de la lista."
    cambios = {}
    if titulo != limpio(actual.get("titulo"), TITULO_MAX):
        cambios["titulo"] = titulo
    if detalle != texto_largo(actual.get("detalle")):
        cambios["detalle"] = detalle or None
    if sev != (actual.get("severidad") or ""):
        cambios["severidad"] = sev
    if not cambios:
        return None, None, "No cambiaste nada."
    motivo, err = validar_motivo(datos.get("motivo"), "la edición")
    if err:
        return None, None, err
    return cambios, motivo, None


def resumen_cambios(cambios, actual, nombres_sev=None):
    """«Gravedad: Alerta → Falla · Título: … → …» para la bitácora (sin plata)."""
    nombres_sev = nombres_sev or {}
    partes = []
    if "severidad" in cambios:
        partes.append("Gravedad: %s → %s" % (nombres_sev.get(actual.get("severidad"), actual.get("severidad") or "—"),
                                             nombres_sev.get(cambios["severidad"], cambios["severidad"])))
    if "titulo" in cambios:
        partes.append("Título: «%s» → «%s»" % (limpio(actual.get("titulo"), 120), limpio(cambios["titulo"], 120)))
    if "detalle" in cambios:
        partes.append("Detalle actualizado")
    return " · ".join(partes)


def validar_borrado(datos, es_superadmin):
    """Borrar de verdad (solo superadmin, para limpiar pruebas). → (motivo, None) o (None, error)."""
    if not es_superadmin:
        return None, "Solo el superadministrador puede borrar un hallazgo."
    datos = datos or {}
    if str(datos.get("confirmar") or "").strip().upper() != CONFIRMAR_BORRAR:
        return None, "Para borrar escribe BORRAR (así nadie borra por un clic)."
    return validar_motivo(datos.get("motivo"), "el borrado")


def _monto(x):
    """'15.990' · '15990' · '$ 15.990' · 15990 → 15990 (pesos chilenos, sin decimales). None si vacío. ValueError si no es un monto."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        n = int(round(float(x)))
    else:
        s = str(x).strip().replace("$", "").replace(" ", "")
        if not s:
            return None
        s = s.replace(".", "").replace(",", "")
        if not s.isdigit():
            raise ValueError("monto")
        n = int(s)
    if n < 0 or n > VALOR_MAX:
        raise ValueError("monto")
    return n


def validar_valor(datos):
    """Valorizar: repuestos y servicio (pesos), y una nota. Al menos uno de los dos montos.
    → ({"repuestos", "servicio", "nota"}, None) o (None, error)."""
    datos = datos or {}
    try:
        rep = _monto(datos.get("repuestos"))
        srv = _monto(datos.get("servicio"))
    except ValueError:
        return None, "Escribe los montos en pesos, solo números (por ejemplo 45.990)."
    if rep is None and srv is None:
        return None, "Escribe al menos un monto: repuestos o servicio."
    return {"repuestos": rep, "servicio": srv, "nota": limpio(datos.get("nota"), NOTA_MAX) or None}, None


def pesos(n):
    """45990 → '$45.990' (formato chileno). '' si no hay monto."""
    if n is None or n == "":
        return ""
    try:
        v = int(round(float(n)))
    except (TypeError, ValueError):
        return ""
    return "$" + format(v, ",").replace(",", ".")


def total_valor(rep, srv):
    if rep is None and srv is None:
        return None
    return int(round(float(rep or 0))) + int(round(float(srv or 0)))


def valor_vista(fila):
    """La valorización lista para dibujar, o None si no hay. `fila`: valor_repuestos, valor_servicio, valor_nota, valorizado_por/_en_txt."""
    if not fila:
        return None
    rep, srv = fila.get("valor_repuestos"), fila.get("valor_servicio")
    tot = total_valor(rep, srv)
    if tot is None:
        return None
    return {"repuestos": pesos(rep) if rep is not None else "", "servicio": pesos(srv) if srv is not None else "",
            "total": pesos(tot), "total_n": tot, "nota": fila.get("valor_nota") or "",
            "por": fila.get("valorizado_por") or "", "cuando": fila.get("valorizado_cuando") or "",
            "rep_n": int(round(float(rep))) if rep is not None else None,
            "srv_n": int(round(float(srv))) if srv is not None else None}


def origen_ref(entidad, eid):
    """'hallazgo:12' / 'inquietud:5' — lo que viaja a «Solicitar repuesto» para dejar el vínculo de vuelta."""
    if entidad not in ENTIDADES or not entero(eid):
        return ""
    return "%s:%d" % (entidad, int(eid))


def leer_origen_ref(s):
    """'hallazgo:12' → ('hallazgo', 12); cualquier otra cosa → (None, None)."""
    m = re.match(r"^(hallazgo|inquietud):(\d{1,10})$", str(s or "").strip())
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def recorrido(estado_ef, ticket, repuesto, seguimiento, valor, tomado):
    """Los pasos de la gestión circular de UN hallazgo, cada uno con su semáforo (REGLA #15): nada se esconde, lo que falta queda a la
    vista. `ticket` / `repuesto` / `seguimiento`: dicts con `numero` (o None). `valor`: valor_vista o None. → lista de pasos."""
    terminado = estado_ef in ("resuelto", "descartado")
    pasos = [
        {"clave": "atencion", "titulo": "Atención", "ok": bool(tomado) or terminado,
         "txt": ("Lo tomó " + tomado) if tomado else ("Descartado" if estado_ef == "descartado" else
                                                       ("Con OT de seguimiento" if estado_ef == "resuelto" else "Nadie lo ha tomado"))},
        {"clave": "valor", "titulo": "Valorización", "ok": bool(valor),
         "txt": ("Estimado " + valor["total"]) if valor else "Sin valorizar"},
        {"clave": "ticket", "titulo": "Ticket", "ok": bool(ticket),
         "txt": ticket["numero"] if ticket else "Sin ticket", "opcional": True},
        {"clave": "repuesto", "titulo": "Repuesto", "ok": bool(repuesto),
         "txt": repuesto["numero"] if repuesto else "Sin solicitud", "opcional": True},
        {"clave": "ot", "titulo": "OT de seguimiento", "ok": bool(seguimiento),
         "txt": seguimiento["numero"] if seguimiento else "Sin OT"},
    ]
    for p in pasos:
        p["color"] = "verde" if p["ok"] else ("gris" if p.get("opcional") or estado_ef == "descartado" else "ambar")
    return pasos

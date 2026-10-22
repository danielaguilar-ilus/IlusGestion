# -*- coding: utf-8 -*-
"""ILUS Fitness · HISTORIA DEL EQUIPO (hoja de vida unificada): funciones puras, sin BD ni Flask.

Daniel (dueño), 2026-10-21: «Si entro a determinado producto, una trotadora de determinada cuenta, me va a decir todos los
diagnósticos que ha tenido, visitas, solicitudes de repuesto, todo bien ordenadito. La OT va a alimentar la ficha y a ordenar y
clasificar la información SIN duplicarla.» Todo mira al N° de serie del equipo.

Esto es una VISTA de lectura: no copia datos a ninguna tabla nueva. Une, en una sola línea de tiempo, lo que YA registra el sistema
(visitas y OT, diagnósticos por equipo, fallas y alertas del checklist, hallazgos, solicitudes de repuesto, fuera de servicio y bajas,
cambios de ficha como el N° de serie, tickets, garantía). Cada hecho aparece UNA sola vez aunque lo registren varias tablas:

  DEDUPLICACIÓN (reglas fijas, probadas en tests/test_maquina_historia.py):
    D1  Un evento de la hoja de vida (mant_maquina_eventos) cuya referencia es una visita, un levantamiento o una solicitud de repuesto
        que ya está en la línea se descarta: la visita / la solicitud lo cuentan completo (con su estado final).
    D2  Los eventos de fotos y «otro» son ruido; los eventos de cambio de estado que escribe el registro de paradas (metadata.parada)
        se descartan: la parada los cuenta con la hora real de inicio y de fin.
    D3  Un hallazgo de una TAREA del checklist reemplaza a la tarea (misma tarea_id): es el mismo hecho, con su estado de gestión.
    D4  Un hallazgo «Falla detectada» (lo que nace del diagnóstico «Con falla») reemplaza a ese diagnóstico en la misma visita.
    D5  Un hallazgo de FUERA DE SERVICIO, BAJA o REPUESTO no se repite: la parada, la baja o la solicitud de repuesto de esa misma visita
        ya lo cuentan, y reciben su estado de gestión («Hallazgo: sin gestionar») y sus fotos. Si no hay con qué cruzarlo, queda solo.
    D6  Un diagnóstico «fuera de servicio» o «dado de baja» (mant_visita_equipos.razon_saltado) lo cuenta la parada o la baja de esa visita.
    D7  Un cambio de la ficha que ya registra una parada (estado_capturado ↔ fuera de servicio / en reparación) o una baja ya contada se
        descarta.
    D8  Un diagnóstico «revisado sin novedad» sin texto ni veredicto NO es una tarjeta: es una línea de la visita (no se inventa un
        diagnóstico que el técnico no escribió).

  ORDEN: del más nuevo al más antiguo por la fecha de INICIO del hecho (un episodio —parada, hallazgo, repuesto— muestra además cuándo
  terminó). Las fechas viven en UTC y se muestran SIEMPRE en hora Chile, día/mes/año (REGLA #6).

  CONFIDENCIALIDAD (REGLAS #19 y #26, no negociable): el servidor es quien quita el dato, no la plantilla.
    · Un técnico (interno, elevado `tecnico_ejecutivo` o externo) NO recibe: hallazgos, tickets, quién hizo nada (salvo el técnico asignado
      a la visita, que ya mostraba la ficha), notas de gestión, ni proveedor, costo, N° de OC o ticket de compra de un repuesto: de la
      solicitud solo sale el nombre de la pieza, la cantidad y su avance en palabras de bodega.
    · «Quién hizo el diagnóstico / la tarea del checklist» es solo del superadministrador (Daniel 2026-08-30: «jamás se enteren o lo vean los
      técnicos o los demás»).
    · Gestión (admin, supervisor, ejecutivo) ve todo lo demás; tampoco recibe montos: esta vista nunca los trae.

Las consultas a la base y las rutas viven en app.py (bloque «HISTORIA DEL EQUIPO»). Aquí: el catálogo, la conversión de cada fila en
una tarjeta, la unificación, los filtros, la paginación y el recorte por rol.
"""
import datetime as _dt
import json
import re

import ot_hallazgos as _oh

# ── Catálogo de tipos (las pastillas de la pantalla) ───────────────────────────────────────────────────────────────────────
# `solo_gestion`: el servidor ni siquiera consulta ni entrega ese tipo a un técnico.
TIPOS = {
    "visita": {"texto": "Visitas y OT", "icono": "bi-clipboard2-pulse-fill", "solo_gestion": False},
    "diagnostico": {"texto": "Diagnósticos", "icono": "bi-clipboard2-check-fill", "solo_gestion": False},
    "checklist": {"texto": "Fallas del checklist", "icono": "bi-list-check", "solo_gestion": False},
    "hallazgo": {"texto": "Hallazgos", "icono": "bi-exclamation-diamond-fill", "solo_gestion": True},
    "repuesto": {"texto": "Repuestos", "icono": "bi-tools", "solo_gestion": False},
    "estado": {"texto": "Estado y bajas", "icono": "bi-activity", "solo_gestion": False},
    "ficha": {"texto": "Cambios en la ficha", "icono": "bi-pencil-square", "solo_gestion": False},
    "ticket": {"texto": "Tickets", "icono": "bi-ticket-detailed-fill", "solo_gestion": True},
    "garantia": {"texto": "Garantía", "icono": "bi-shield-check", "solo_gestion": False},
}
ORDEN_TIPOS = tuple(TIPOS)

# Semáforo (REGLA #2/#15): verde ok · ámbar hay algo que mirar · rojo falla REAL · gris informativo.
SEVERIDADES = ("ok", "aviso", "falla", "info")

POR_PAGINA_OPCIONES = (10, 20, 50, 100)
POR_PAGINA_DEFECTO = 20
FOTOS_MAX = 6

MARCA_RIESGO = "⚠ PUEDE LASTIMAR A ALGUIEN"      # el mismo texto que app.py::_FS_MARCA_RIESGO (una prueba los compara)

_MESES = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")

# ── Textos de las fuentes ──────────────────────────────────────────────────────────────────────────────────────────────────
TIPO_OT_TEXTO = {
    "preventiva": "Mantención preventiva", "correctiva": "Correctiva", "garantia": "Garantía", "inspeccion": "Inspección",
    "levantamiento": "Levantamiento", "instalacion": "Instalación", "visita_tecnica": "Visita técnica",
    "visita_correctiva": "Visita correctiva", "cambio_equipo": "Cambio de equipo", "desinstalacion": "Desinstalación",
    "capacitacion": "Capacitación", "repuesto": "Instalación de repuesto", "revision_interna": "Revisión interna",
    "control_calidad": "Control de calidad", "movimiento_equipos": "Movimiento de equipos", "trabajo_interno": "Trabajo interno",
}
# Estado de la OT → (texto, semáforo). Mismos estados que app.py::_OT2_ESTADO_META.
ESTADO_OT = {
    "creada": ("Creada", "info"), "programada": ("Agendada", "info"), "asignada": ("Asignada", "info"),
    "reagendada": ("Reagendada", "info"), "en_curso": ("En curso", "info"), "en_ejecucion": ("En curso", "info"),
    "firmada_tecnico": ("Firmada por el técnico", "ok"), "pendiente_info": ("Falta información", "aviso"),
    "pendiente_repuesto": ("Falta repuesto", "aviso"), "pendiente_aprobacion": ("Por aprobar", "ok"),
    "completada": ("Completada", "ok"), "cerrada": ("Cerrada", "ok"), "cancelada": ("Cancelada", "info"),
    "anulada": ("Anulada", "info"),
}
# OT abiertas = ya empezó el trabajo y todavía no se cierra: la pantalla las marca «en curso» (Daniel: abiertas y cerradas, las dos se ven).
OT_EN_CURSO = ("en_curso", "en_ejecucion", "firmada_tecnico", "pendiente_info", "pendiente_repuesto", "pendiente_aprobacion")
OT_CERRADAS = ("cerrada", "completada", "cancelada", "anulada")
REVISION_TEXTO = {
    "verificado": "Revisado sin novedad", "con_cambios": "Revisado con cambios", "saltado": "No se pudo revisar",
    "falla_detectada": "Con falla detectada",
}
RAZON_SALTADO_TEXTO = {
    "no_encontrado": "No se encontró el equipo", "dado_de_baja": "Dado de baja", "danado_inaccesible": "Dañado o inaccesible",
    "dañado_inaccesible": "Dañado o inaccesible", "cliente_lo_quito": "El cliente lo retiró", "otro": "Otro motivo",
    "sin_acceso": "Sin acceso al equipo", "cliente_no_permite": "El cliente no lo permitió", "equipo_en_uso": "Equipo en uso",
    "sin_energia": "Sin energía", "requiere_repuesto": "Requiere repuesto", "reagendar": "Para reagendar", "no_aplica": "No aplica",
    "falta_stock": "No llegó · falta stock", "fuera_de_servicio": "Fuera de servicio",
}
DIAGNOSTICO_TEXTO = {"aprobado": ("Aprobado", "ok"), "observacion": ("Con observación", "aviso"), "falla": ("Con falla", "falla")}

# Repuestos: avance en palabras de bodega. NUNCA «proveedor», costo, OC ni ticket de compra (REGLAS #19/#26).
REPUESTO_ABIERTOS = ("solicitado", "validado", "pedido", "recibido")
REPUESTO_TEXTO_GESTION = {
    "solicitado": "Solicitado, por revisar en bodega", "validado": "Validado en bodega", "pedido": "Pedido, en camino a bodega",
    "recibido": "Recibido en bodega, falta instalarlo", "instalado": "Instalado", "rechazado": "Anulado",
}
REPUESTO_TEXTO_TECNICO = {
    "solicitado": "Solicitado", "validado": "En revisión de bodega", "pedido": "En revisión de bodega",
    "recibido": "Pieza lista en bodega", "instalado": "Instalado", "rechazado": "Anulado",
}
# Lo que falta para cerrarlo (el paso pendiente de la tarjeta).
REPUESTO_FALTA_GESTION = {
    "solicitado": "Falta la revisión de bodega", "validado": "Falta que llegue la pieza", "pedido": "Falta que llegue la pieza",
    "recibido": "Falta instalarla",
}
REPUESTO_FALTA_TECNICO = {
    "solicitado": "Falta la revisión de bodega", "validado": "Falta que la pieza esté lista", "pedido": "Falta que la pieza esté lista",
    "recibido": "Falta instalarla",
}
REPUESTO_ORIGEN_TEXTO = {"piola": "Cambio de piola", "cinta": "Cambio de cinta"}

# Tickets (solo gestión).
TICKET_ESTADO_TEXTO = {
    "open": "Abierto", "in_progress": "En curso", "pending": "Pendiente", "resolved": "Resuelto", "closed": "Cerrado",
    "ot_pending_approval": "OT por aprobar", "ot_generated": "OT generada", "ot_in_progress": "OT en curso", "cancelado": "Cancelado",
}
TICKET_CERRADOS = ("resolved", "closed", "cancelado")

# Campos de la ficha que se auditan (mant_maquina_audit.campo) y cómo se leen.
AUDIT_DATO_TEXTO = {"nombre": "el nombre", "sku": "el SKU", "marca": "la marca", "modelo": "el modelo", "voltaje": "el voltaje"}
ESTADO_FICHA_TEXTO = {"activo": "Activo", "inactivo": "Inactivo", "baja": "Dado de baja", "garantia": "En garantía"}
ESTADO_OP_TEXTO = {
    "operativo": "Operativo", "advertencia": "Con advertencia", "critico": "Crítico", "fuera_servicio": "Fuera de servicio",
    "en_mantencion": "En mantención", "en_reparacion": "En reparación", "dado_baja": "Dado de baja", "no_encontrado": "No encontrado",
}
ESTADOS_PARADA = ("fuera_servicio", "en_reparacion")      # estado_capturado que abre una parada (app.py::_MAQ_ESTADOS_PARADA)


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Rol
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class Rol:
    """Quién mira. `es_tecnico` = toda la familia técnico (interno, elevado y externo: app.py::_es_rol_tecnico()). `es_superadmin` =
    el único que ve quién hizo cada diagnóstico y cada tarea del checklist."""
    __slots__ = ("es_tecnico", "es_superadmin")

    def __init__(self, es_tecnico=False, es_superadmin=False):
        self.es_tecnico = bool(es_tecnico)
        self.es_superadmin = bool(es_superadmin) and not self.es_tecnico

    @property
    def gestion(self):
        return not self.es_tecnico

    def __repr__(self):
        return "Rol(tecnico=%s, superadmin=%s)" % (self.es_tecnico, self.es_superadmin)


def tipos_permitidos(rol):
    """Las claves de TIPOS que este rol puede recibir, en el orden de las pastillas."""
    return tuple(k for k in ORDEN_TIPOS if not (rol.es_tecnico and TIPOS[k]["solo_gestion"]))


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Utilidades de texto, números y fechas
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def txt(x, tope=400):
    """Una línea limpia (espacios simples). El tope es solo una red de seguridad: los nombres y títulos van COMPLETOS (REGLA #15)."""
    return " ".join(str(x if x is not None else "").split())[:tope]


def txt_largo(x, tope=3000):
    """Un texto de varias líneas (conserva los saltos de línea; quita espacios sobrantes y líneas vacías)."""
    lineas = [" ".join(l.split()) for l in str(x if x is not None else "").splitlines()]
    return "\n".join(l for l in lineas if l)[:tope]


def entero(x):
    return _oh.entero(x)


def numero(x, defecto=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return defecto


def cantidad_txt(c):
    n = numero(c, 1.0)
    return "%g" % n if n else "1"


_TZ = {"zona": None, "intentado": False}


def _zona():
    if not _TZ["intentado"]:
        _TZ["intentado"] = True
        try:
            from zoneinfo import ZoneInfo
            _TZ["zona"] = ZoneInfo("America/Santiago")
        except Exception:
            _TZ["zona"] = None
    return _TZ["zona"]


def a_chile(ts):
    """datetime naive en UTC (como lo guarda MySQL con NOW()) → datetime naive en hora de Chile (con horario de verano). None si no es fecha."""
    if not isinstance(ts, _dt.datetime):
        return None
    z = _zona()
    if z is not None:
        return ts.replace(tzinfo=_dt.timezone.utc).astimezone(z).replace(tzinfo=None)
    return ts - _dt.timedelta(hours=4)


def chile_a_utc(local):
    """datetime naive en hora de Chile → datetime naive en UTC."""
    z = _zona()
    if z is not None:
        return local.replace(tzinfo=z).astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return local + _dt.timedelta(hours=4)


def _hora_de(x):
    """(hora, minuto) de una columna TIME (timedelta en pymysql, time o 'HH:MM[:SS]'), o None."""
    try:
        if isinstance(x, _dt.timedelta):
            seg = int(x.total_seconds())
            return (seg // 3600) % 24, (seg % 3600) // 60
        if isinstance(x, _dt.time):
            return x.hour, x.minute
        if isinstance(x, str) and re.match(r"^\d{1,2}:\d{2}", x.strip()):
            h, m = x.strip().split(":")[:2]
            return int(h) % 24, int(m)
    except (TypeError, ValueError):
        pass
    return None


def instante(valor, hora=None):
    """Convierte lo que trae una columna a (ts_utc, solo_fecha):
       · datetime  → es un instante UTC tal cual (solo_fecha=False);
       · date      → un DÍA de Chile: sin hora conocida cae al mediodía (solo_fecha=True); con `hora` (TIME) se usa esa hora local;
       · otra cosa → (None, False)."""
    if isinstance(valor, _dt.datetime):
        return valor, False
    if isinstance(valor, _dt.date):
        hm = _hora_de(hora)
        local = _dt.datetime(valor.year, valor.month, valor.day, hm[0] if hm else 12, hm[1] if hm else 0)
        return chile_a_utc(local), hm is None
    return None, False


def fmt_fecha(ts, solo_fecha=False):
    """«07/10/2026 14:30» (hora Chile) o «07/10/2026». Vacío si no hay fecha."""
    c = a_chile(ts)
    if c is None:
        return ""
    return c.strftime("%d/%m/%Y") if solo_fecha else c.strftime("%d/%m/%Y %H:%M")


def dia_chile(ts):
    c = a_chile(ts)
    return c.date() if c else None


def mes_clave(ts):
    c = a_chile(ts)
    return c.strftime("%Y-%m") if c else ""


def mes_texto(ts):
    c = a_chile(ts)
    return "%s de %d" % (_MESES[c.month - 1], c.year) if c else ""


def fmt_duracion(horas):
    """Lo mismo que app.py::_hv_fmt_dur: «35 min» · «3 h 20 min» · «4 días 2 h»."""
    try:
        h = float(horas or 0)
    except (TypeError, ValueError):
        return "—"
    if h < 1:
        return "%d min" % max(1, int(round(h * 60)))
    if h < 48:
        hh, mm = int(h), int(round((h - int(h)) * 60))
        return "%d h" % hh + (" %d min" % mm if mm else "")
    d = int(h // 24)
    return "%d días" % d + (" %d h" % int(h - d * 24) if int(h - d * 24) else "")


def hace_texto(ts, ahora):
    """«hoy» · «hace 1 día» · «hace 12 días» · «hace 3 meses»."""
    if not isinstance(ts, _dt.datetime) or not isinstance(ahora, _dt.datetime):
        return ""
    d = (ahora - ts).days
    if d < 0:
        return "en %d día%s" % (-d, "" if d == -1 else "s")
    if d == 0:
        return "hoy"
    if d == 1:
        return "hace 1 día"
    if d < 60:
        return "hace %d días" % d
    m = d // 30
    return "hace %d meses" % m if m < 24 else "hace %d años" % (m // 12)


def fecha_iso(texto):
    """'2026-10-21' → date; cualquier otra cosa → None (los filtros de fecha nunca explotan)."""
    if isinstance(texto, _dt.datetime):
        return texto.date()
    if isinstance(texto, _dt.date):
        return texto
    if isinstance(texto, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto.strip()):
        try:
            return _dt.date(*[int(p) for p in texto.strip().split("-")])
        except ValueError:
            return None
    return None


def numero_ot(vid, numeros):
    return (numeros or {}).get(int(vid)) or "V-%05d" % int(vid)


def url_ot(vid, visibles):
    """/ot/<id>, o None si el rol no puede entrar a esa OT (`visibles` None = puede con todas)."""
    if vid is None:
        return None
    if visibles is not None and int(vid) not in visibles:
        return None
    return "/ot/%d" % int(vid)


def _ref_ot(vid, numeros, visibles, texto="Ver OT"):
    num = numero_ot(vid, numeros)
    return {"tabla": "mant_visitas", "id": int(vid), "url": url_ot(vid, visibles), "numero": num, "texto": "%s %s" % (texto, num)}


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  La tarjeta
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def nueva(id_, tipo, subtipo, severidad, titulo, ts, solo_fecha=False, detalle=""):
    """Una tarjeta vacía con todas sus claves. Las que empiezan con «_» son internas (no viajan al navegador)."""
    return {
        "id": id_, "tipo": tipo, "subtipo": subtipo, "severidad": severidad if severidad in SEVERIDADES else "info",
        "titulo": txt(titulo, 600), "detalle": txt_largo(detalle), "ts": ts, "solo_fecha": bool(solo_fecha),
        "fin_ts": None, "fin_solo_fecha": False, "abierto": False, "estado_txt": "", "estado_color": "",
        "lineas": [], "pasos": [], "ref": None, "refs": [], "actor": None, "gestion": None, "riesgo": False,
        # Las cuatro respuestas de cada hecho de terreno: QUÉ (el título), POR QUÉ (`por_que`; si el hecho lo exige y no hay, la pantalla dice
        # «sin motivo registrado» en gris, nunca lo inventa), CUÁNDO (la fecha) y la OT (`ref`).
        "por_que": "", "pide_motivo": False,
        "_fotos": [], "_visita_id": None, "_tarea_id": None, "_solicitud_id": None, "_hallazgo_id": None,
        "_hallazgo_sev": None, "_label": "", "_referencia": "", "_razon": "", "_parada_visitas": (), "_baja": False,
    }


def _linea(entrada, icono, texto):
    t = txt(texto, 400)
    if t:
        entrada["lineas"].append({"icono": icono, "texto": t})


def _paso(entrada, texto, ts=None, solo_fecha=False, hecho=True, actual=False):
    entrada["pasos"].append({"texto": txt(texto, 200), "fecha": fmt_fecha(ts, solo_fecha) if ts else "", "hecho": bool(hecho),
                             "actual": bool(actual)})


def _actor(rol, valor, solo_superadmin=False):
    """Quién hizo algo: un técnico nunca lo recibe; el «quién» de diagnósticos y checklist solo el superadministrador."""
    if rol.es_tecnico:
        return None
    if solo_superadmin and not rol.es_superadmin:
        return None
    return txt(valor, 190) or None


def _quitar(lista, tarjeta):
    """Saca `tarjeta` de `lista` comparando por IDENTIDAD (nunca por igualdad de diccionarios)."""
    for i, x in enumerate(lista):
        if x is tarjeta:
            del lista[i]
            return True
    return False


def _esta(lista, tarjeta):
    return any(x is tarjeta for x in lista)


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Fila → tarjeta (una función por fuente)
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def diagnostico_genera_tarjeta(f):
    """¿Esta fila de mant_visita_equipos merece su propia tarjeta? Solo si la persona dijo algo: un veredicto, un estado distinto de
    «verificado» o un texto. Un «verificado» a secas (el cierre automático lo crea para todos) NO es un diagnóstico (D8)."""
    if not f.get("revisado_at"):
        return False
    if f.get("diagnostico_estado"):
        return True
    if (f.get("estado_revision") or "") in ("saltado", "falla_detectada", "con_cambios"):
        return True
    return bool(txt(f.get("observacion_tecnico")) or txt(f.get("diagnostico_texto")))


OT_RELACION_PORQUE = {"seguimiento": "Seguimiento de %s: lo que quedó por resolver en esa OT", "pendiente_stock": "Quedó pendiente por falta de stock en %s",
                      "desinstalacion": "Desinstalación ligada a %s"}


def tarjeta_visita(v, rol, numeros, visibles, rev=None, ahora=None):
    """Una OT que tocó el equipo. `rev` = su fila de mant_visita_equipos si NO tiene tarjeta de diagnóstico (se dice en una línea)."""
    vid = int(v["id"])
    tipo = (v.get("tipo") or "").strip().lower()
    estado = (v.get("estado") or "").strip().lower()
    est_txt, sev = ESTADO_OT.get(estado, (estado.replace("_", " ").capitalize() or "—", "info"))
    ts, solo_fecha = instante(v.get("hora_real_inicio"))
    if ts is None:
        ts, solo_fecha = instante(v.get("fecha_programada"), v.get("hora_inicio"))
    if ts is None:
        ts, solo_fecha = instante(v.get("fecha_realizada"))
    if ts is None:
        ts, solo_fecha = instante(v.get("cerrada_at"))
    if ts is None:
        return None
    label = TIPO_OT_TEXTO.get(tipo) or (tipo.replace("_", " ").capitalize() if tipo else "Visita")
    e = nueva("visita:%d" % vid, "visita", tipo or "visita", sev, "%s · %s" % (numero_ot(vid, numeros), label), ts, solo_fecha,
              detalle=v.get("titulo") or "")
    e["_visita_id"] = vid
    e["_label"] = label
    e["estado_txt"], e["estado_color"] = est_txt, {"ok": "verde", "aviso": "ambar", "info": "gris"}.get(sev, "gris")
    e["abierto"] = estado in OT_EN_CURSO
    if e["abierto"]:
        e["estado_txt"] = ("En curso · %s" % est_txt) if est_txt != "En curso" else "En curso"
        e["estado_color"] = "azul"
    e["ref"] = _ref_ot(vid, numeros, visibles)
    e["pide_motivo"] = True
    rel0 = (v.get("ot_relacion") or "").strip().lower()
    madre0 = entero(v.get("ot_madre_id"))
    if madre0 and rel0 in OT_RELACION_PORQUE:
        e["por_que"] = OT_RELACION_PORQUE[rel0] % numero_ot(madre0, numeros)
    elif txt(v.get("descripcion")):
        e["por_que"] = txt(v["descripcion"], 400)
    if txt(v.get("tecnico_nombre")):
        _linea(e, "bi-person-badge", "Técnico: %s" % txt(v["tecnico_nombre"], 150))
    rel = (v.get("ot_relacion") or "").strip().lower()
    madre = entero(v.get("ot_madre_id"))
    if madre and rel in ("seguimiento", "pendiente_stock", "desinstalacion"):
        _linea(e, "bi-diagram-3-fill", {"seguimiento": "Seguimiento de %s", "pendiente_stock": "Pendiente por falta de stock de %s",
                                        "desinstalacion": "Desinstalación de %s"}[rel] % numero_ot(madre, numeros))
    if v.get("cerrada_at") and estado in ("cerrada", "completada"):
        _linea(e, "bi-calendar-check", "Cerrada el %s" % fmt_fecha(v["cerrada_at"]))
    if rev and rev.get("estado_revision") in REVISION_TEXTO and rev.get("estado_revision") != "saltado":
        _linea(e, "bi-clipboard2-check", "En esta visita: %s" % REVISION_TEXTO[rev["estado_revision"]].lower())
    if v.get("_levantamiento"):
        _linea(e, "bi-clipboard-data", "Se levantó la ficha del equipo en esta visita")
    if isinstance(ahora, _dt.datetime) and ts > ahora:
        e["estado_txt"] = "%s · próxima" % est_txt
    return e


def tarjeta_diagnostico(f, rol, numeros, visibles):
    """El veredicto sobre ESTE equipo en una visita (mant_visita_equipos)."""
    vid = int(f["visita_id"])
    ts, _ = instante(f.get("revisado_at"))
    if ts is None:
        return None
    veredicto = (f.get("diagnostico_estado") or "").strip().lower()
    rev = (f.get("estado_revision") or "").strip().lower()
    razon = (f.get("razon_saltado") or "").strip().lower()
    texto = txt_largo(f.get("diagnostico_texto"))
    obs = txt_largo(f.get("observacion_tecnico"))
    if veredicto in DIAGNOSTICO_TEXTO:
        etiqueta, sev = DIAGNOSTICO_TEXTO[veredicto]
        titulo, sub = "Diagnóstico: %s" % etiqueta, veredicto
    elif rev == "saltado":
        titulo, sub, sev = "No se pudo revisar: %s" % RAZON_SALTADO_TEXTO.get(razon, razon.replace("_", " ") or "sin motivo"), "no_revisado", "aviso"
    elif rev == "falla_detectada":
        titulo, sub, sev = "Diagnóstico: Con falla", "falla", "falla"
    elif rev == "con_cambios":
        titulo, sub, sev = "Revisado con cambios", "cambios", "info"
    else:
        titulo, sub, sev = "Diagnóstico: Revisado", "aprobado", "ok"
    detalle = texto or obs
    if texto and obs and obs != texto:
        detalle = texto + "\n" + obs
    e = nueva("diag:%d" % vid, "diagnostico", sub, sev, titulo, ts, False, detalle=detalle)
    e["pide_motivo"] = sev != "ok"
    e["_visita_id"] = vid
    e["_razon"] = razon
    e["ref"] = _ref_ot(vid, numeros, visibles)
    e["actor"] = _actor(rol, f.get("revisado_por"), solo_superadmin=True)
    return e


def tarjeta_checklist(t, ev, ts_visita, rol, numeros, visibles):
    """Una tarea del checklist de este equipo que quedó en Falla o Alerta (`ev` = ot_hallazgos.evaluar_tarea)."""
    tid = int(t["id"])
    vid = int(t["visita_id"])
    ts, _ = instante(t.get("completada_at"))
    if ts is None:
        ts = ts_visita
    if ts is None:
        return None
    sev_h = ev["severidad"]
    sev = "aviso" if sev_h == _oh.SEV_ALERTA else "falla"
    titulo = _oh.titulo_tarea("", t.get("titulo"), ev.get("valor_txt"))
    e = nueva("tarea:%d" % tid, "checklist", sev_h, sev, titulo, ts, False, detalle=t.get("observaciones") or "")
    e["pide_motivo"] = True
    e["_visita_id"], e["_tarea_id"] = vid, tid
    e["ref"] = _ref_ot(vid, numeros, visibles)
    e["actor"] = _actor(rol, t.get("completada_por"), solo_superadmin=True)
    return e


def estado_hallazgo(h):
    """(estado_efectivo, seguimiento_caido) con la misma regla del módulo de hallazgos."""
    return _oh.estado_efectivo(h, h.get("seg_numero") or ("x" if h.get("seg_estado") else None), h.get("seg_estado"))


def tarjeta_hallazgo(h, rol, numeros, visibles):
    """Lo que el técnico reportó y la gestión sigue (mant_ot_hallazgos). SOLO gestión: `construir` ni lo invoca para un técnico."""
    hid = int(h["id"])
    vid = int(h["visita_id"])
    ts, _ = instante(h.get("detectado_en"))
    if ts is None:
        return None
    estado, caido = estado_hallazgo(h)
    sev_clave = h.get("severidad") if h.get("severidad") in _oh.SEVERIDADES else _oh.SEV_ALERTA
    info = _oh.sev_info(sev_clave)
    seg_id = entero(h.get("ot_seguimiento_id"))
    seg_ok = bool(seg_id) and not caido
    seg_cerrada = seg_ok and (h.get("seg_estado") or "") in ("cerrada", "completada")
    cerrado_con_ot = estado == _oh.EST_RESUELTO and seg_cerrada
    if cerrado_con_ot:
        sev = "ok"                                                  # lo arregló su OT de seguimiento
    elif estado == _oh.EST_DESCARTADO:
        sev = "info"
    elif estado == _oh.EST_RESUELTO:
        sev = "aviso"                                               # ya hay una OT de seguimiento en curso: se está atendiendo
    else:
        sev = "falla" if info["prioridad"] == "alta" else "aviso"   # sin gestionar o con el caso tomado: sigue siendo lo que es
    e = nueva("hallazgo:%d" % hid, "hallazgo", sev_clave, sev, h.get("titulo") or info["texto"], ts, False, detalle=h.get("detalle") or "")
    e["pide_motivo"] = True
    e["_visita_id"], e["_hallazgo_id"], e["_hallazgo_sev"] = vid, hid, sev_clave
    e["_tarea_id"] = entero(h.get("tarea_id")) if (h.get("origen") == _oh.ORIGEN_TAREA) else None
    e["_referencia"] = h.get("referencia") or ""
    e["riesgo"] = bool(h.get("riesgo"))
    e["abierto"] = estado in (_oh.EST_ABIERTO, _oh.EST_TOMADO) or (estado == _oh.EST_RESUELTO and not seg_cerrada)
    if estado == _oh.EST_DESCARTADO and isinstance(h.get("descartado_en"), _dt.datetime):
        e["fin_ts"] = h["descartado_en"]
    elif cerrado_con_ot and isinstance(h.get("seg_cerrada"), _dt.datetime):
        e["fin_ts"] = h["seg_cerrada"]
    est_info = _oh.estado_info(estado)
    e["estado_txt"], e["estado_color"] = est_info["texto"], est_info["color"]
    e["ref"] = _ref_ot(vid, numeros, visibles)
    quien = _actor(rol, h.get("detectado_por"))
    e["actor"] = quien
    e["_fotos"] = _oh.lista_urls(h.get("fotos_json"))
    # Seguimiento en pasos: reportado → caso tomado → OT de seguimiento / descartado.
    _paso(e, "Reportado" + (" por %s" % txt(h.get("detectado_por"), 100) if quien else ""), ts)
    if h.get("tomado_en") and estado in (_oh.EST_TOMADO, _oh.EST_RESUELTO):
        _paso(e, "Caso tomado" + (" por %s" % txt(h.get("tomado_por"), 100) if txt(h.get("tomado_por")) else ""), h.get("tomado_en"))
    if seg_ok:
        num = h.get("seg_numero") or numero_ot(seg_id, numeros)
        _paso(e, "OT de seguimiento %s%s" % (num, " (cerrada)" if seg_cerrada else " en curso"),
              h.get("seg_cerrada") if seg_cerrada else h.get("seguimiento_en"), hecho=seg_cerrada, actual=not seg_cerrada)
        ref = _ref_ot(seg_id, numeros, visibles, texto="Ver OT de seguimiento")
        ref["numero"], ref["texto"] = num, "Ver OT de seguimiento %s" % num
        e["refs"].append(ref)
    if caido:
        _paso(e, "La OT de seguimiento %s se canceló: volvió a quedar sin gestionar" % (h.get("seg_numero") or ""), None, hecho=False, actual=True)
    if estado == _oh.EST_DESCARTADO:
        _paso(e, "Descartado" + (" por %s" % txt(h.get("descartado_por"), 100) if txt(h.get("descartado_por")) else ""), h.get("descartado_en"))
        if txt(h.get("descarte_motivo")):
            _linea(e, "bi-chat-left-text", "Motivo: %s" % txt(h["descarte_motivo"], 400))
    e["refs"].append({"tabla": "mant_ot_hallazgos", "id": hid, "numero": "", "texto": "Ver hallazgo",
                      "url": ("/ot/hallazgos?ot=%d#hz%d" % (vid, hid)) if url_ot(vid, visibles) else None})
    return e


def gestion_de(hallazgo_tarjeta):
    """El estado de gestión de un hallazgo, para pegarlo en la tarjeta que ya cuenta el hecho (D5)."""
    seg = None
    for r in hallazgo_tarjeta["refs"]:
        if r.get("tabla") == "mant_visitas":
            seg = dict(r)
            break
    return {"texto": "Hallazgo: %s" % hallazgo_tarjeta["estado_txt"].lower(), "color": hallazgo_tarjeta["estado_color"] or "ambar",
            "estado": hallazgo_tarjeta["estado_txt"], "seguimiento": seg}


def tarjeta_solicitud(s, rol, numeros, visibles):
    """Una solicitud de repuesto de este equipo, con su avance. Solo nombre de pieza y cantidad (REGLAS #19/#26)."""
    sid = int(s["id"])
    ts, _ = instante(s.get("created_at"))
    if ts is None:
        return None
    estado = (s.get("estado") or "").strip().lower()
    abierta = estado in REPUESTO_ABIERTOS
    textos = REPUESTO_TEXTO_TECNICO if rol.es_tecnico else REPUESTO_TEXTO_GESTION
    nombre = txt(s.get("repuesto_nombre"), 300) or "Repuesto"
    cant = cantidad_txt(s.get("cantidad"))
    origen = (s.get("origen") or "").strip().lower()
    sev = "ok" if estado == "instalado" else ("info" if estado == "rechazado" else "aviso")
    fin = None
    if estado == "instalado":
        fin = s.get("instalado_at") or s.get("updated_at")
    elif estado == "rechazado":
        fin = s.get("updated_at")
    titulo = "%s: %s × %s" % (REPUESTO_ORIGEN_TEXTO.get(origen, "Repuesto"), nombre, cant)
    e = nueva("sol:%d" % sid, "repuesto", origen if origen in REPUESTO_ORIGEN_TEXTO else "repuesto", sev, titulo, ts, False,
              detalle=s.get("motivo") or "")
    vid = entero(s.get("visita_id"))
    e["pide_motivo"] = True
    e["_visita_id"], e["_solicitud_id"] = vid, sid
    e["abierto"] = abierta
    e["fin_ts"] = fin if isinstance(fin, _dt.datetime) else None
    e["estado_txt"] = textos.get(estado, estado.replace("_", " ").capitalize() or "—")
    e["estado_color"] = {"ok": "verde", "aviso": "ambar", "info": "gris"}[sev]
    if txt(s.get("repuesto_sku")):
        _linea(e, "bi-upc-scan", "SKU %s" % txt(s["repuesto_sku"], 80))
    if s.get("dejo_fuera_servicio"):
        _linea(e, "bi-sign-stop-fill", "Dejó el equipo fuera de servicio")
    # Avance: el técnico ve 3 pasos (sin «validado» ni «pedido»); la gestión, todos.
    _paso(e, "Solicitado", s.get("created_at"))
    if not rol.es_tecnico:
        if s.get("validado_at"):
            _paso(e, "Validado en bodega", s.get("validado_at"))
        if s.get("pedido_at"):
            _paso(e, "Pedido, en camino a bodega", s.get("pedido_at"))
    if s.get("recibido_at"):
        _paso(e, "Pieza lista en bodega", s.get("recibido_at"))
    if estado == "instalado":
        _paso(e, "Instalado", s.get("instalado_at") or s.get("updated_at"))
    elif estado == "rechazado":
        _paso(e, "Anulado", s.get("updated_at"))
    elif abierta:
        _paso(e, REPUESTO_FALTA_TECNICO.get(estado, "En curso") if rol.es_tecnico else REPUESTO_FALTA_GESTION.get(estado, "En curso"),
              None, hecho=False, actual=True)
    if vid:
        e["ref"] = _ref_ot(vid, numeros, visibles, texto="Ver OT del pedido")
    gen = entero(s.get("ot_generada_id"))
    if gen and gen != vid:
        e["refs"].append(_ref_ot(gen, numeros, visibles, texto="Ver OT de instalación"))
    e["actor"] = _actor(rol, s.get("solicitado_por"))
    return e


def _riesgo_y_motivo(motivo):
    m = txt(motivo, 500)
    riesgo = m.startswith(MARCA_RIESGO)
    if riesgo:
        m = m[len(MARCA_RIESGO):].lstrip(" ·").strip()
    return riesgo, m


def tarjeta_parada(p, rol, numeros, visibles, ahora=None):
    """Un episodio de fuera de servicio (mant_maquina_paradas): desde cuándo y hasta cuándo, o cómo terminó en baja."""
    pid = int(p["id"])
    ts, _ = instante(p.get("inicio_at"))
    if ts is None:
        return None
    fin = p.get("fin_at") if isinstance(p.get("fin_at"), _dt.datetime) else None
    riesgo, motivo = _riesgo_y_motivo(p.get("motivo"))
    ref_ini = entero(p.get("ref_id")) if (p.get("ref_tabla") == "mant_visitas") else None
    ref_fin = entero(p.get("ref_fin_id")) if (p.get("ref_fin_tabla") == "mant_visitas") else None
    aprox = bool(p.get("inicio_aprox"))
    horas = ((fin or ahora or _dt.datetime.utcnow()) - ts).total_seconds() / 3600.0
    dur = ("≈ " if aprox else "") + fmt_duracion(max(horas, 0))
    baja = (p.get("estado_fin") or "") == "dado_baja" and fin is not None
    en_rep = (p.get("estado_inicio") or "") == "en_reparacion"
    base = "En reparación" if en_rep else "Fuera de servicio"
    if baja:
        det = "Estuvo %s %s antes de la baja." % (base.lower(), dur) + ((" Motivo: " + motivo) if motivo else "")
        e = nueva("parada:%d" % pid, "estado", "baja", "falla", "Dado de baja", fin, False, detalle=det)
        e["_baja"] = True
        e["pide_motivo"], e["por_que"] = True, motivo
        e["_visita_id"] = ref_fin or ref_ini
        e["estado_txt"], e["estado_color"] = "Dado de baja", "negro"
        _paso(e, "Quedó %s%s" % (base.lower(), " (inicio estimado)" if aprox else ""), ts)
        _paso(e, "Dado de baja", fin)
        vid_ref = ref_fin or ref_ini
    else:
        titulo = base + (": " + motivo if motivo else "")
        e = nueva("parada:%d" % pid, "estado", "en_reparacion" if en_rep else "fuera_servicio", "ok" if fin else "falla", titulo, ts, False)
        e["pide_motivo"], e["por_que"] = True, motivo
        e["_visita_id"] = ref_ini
        e["fin_ts"] = fin
        e["abierto"] = fin is None
        e["estado_txt"] = ("Volvió a operar tras %s" % dur) if fin else ("Sigue detenido · %s" % dur)
        e["estado_color"] = "verde" if fin else "rojo"
        e["riesgo"] = riesgo
        _paso(e, "Quedó %s%s" % (base.lower(), " (inicio estimado)" if aprox else ""), ts)
        if fin:
            _paso(e, "Volvió a operar", fin)
        else:
            _paso(e, "Sigue detenido", None, hecho=False, actual=True)
        vid_ref = ref_ini
    e["_parada_visitas"] = tuple(x for x in (ref_ini, ref_fin) if x)
    if vid_ref:
        e["ref"] = _ref_ot(vid_ref, numeros, visibles)
    if ref_fin and ref_fin != vid_ref:
        e["refs"].append(_ref_ot(ref_fin, numeros, visibles, texto="Ver OT donde terminó"))
    e["actor"] = _actor(rol, p.get("cerrada_por") if (fin and p.get("cerrada_por")) else p.get("abierta_por"))
    return e


# Eventos de la hoja de vida (mant_maquina_eventos): qué tipo de tarjeta son. Los que no están aquí (fotos, «otro») son ruido (D2).
_EVENTO_TIPO = {
    "instalacion": ("visita", "instalacion", "info"), "levantamiento": ("visita", "levantamiento", "info"),
    "visita": ("visita", "visita", "info"), "reparacion": ("visita", "reparacion", "info"),
    "reubicacion": ("ficha", "ubicacion", "info"), "cambio_repuesto": ("repuesto", "repuesto", "ok"),
    "cambio_estado": ("estado", "cambio", "info"), "dado_baja": ("estado", "baja", "falla"), "garantia": ("garantia", "garantia", "info"),
}


def _json_dict(x):
    if isinstance(x, dict):
        return x
    if not x:
        return {}
    try:
        v = json.loads(x)
        return v if isinstance(v, dict) else {}
    except (TypeError, ValueError):
        return {}


def tarjeta_evento(ev, rol, numeros, visibles):
    """Un evento de la hoja de vida que ninguna otra tarjeta cuenta (la deduplicación decide si llega aquí)."""
    clave = (ev.get("tipo") or "").strip().lower()
    if clave not in _EVENTO_TIPO:
        return None
    tipo, sub, sev = _EVENTO_TIPO[clave]
    ts, _ = instante(ev.get("fecha_evento"))
    if ts is None:
        return None
    e = nueva("evento:%d" % int(ev["id"]), tipo, sub, sev, ev.get("descripcion") or TIPOS[tipo]["texto"], ts, False)
    e["pide_motivo"] = tipo in ("visita", "estado", "repuesto")
    if sub == "baja":
        e["_baja"] = True
        e["estado_txt"], e["estado_color"] = "Dado de baja", "negro"
    ref_i = entero(ev.get("referencia_id"))
    if ev.get("referencia_tabla") == "mant_visitas" and ref_i:
        e["_visita_id"] = ref_i
        e["ref"] = _ref_ot(ref_i, numeros, visibles)
    e["actor"] = _actor(rol, ev.get("created_by"))
    return e


def tarjeta_auditoria(a, rol):
    """Un cambio de la ficha (mant_maquina_audit): N° de serie, ubicación, datos del equipo o estado. None si no es de los que se muestran."""
    campo = (a.get("campo") or "").strip().lower()
    ts, _ = instante(a.get("fecha"))
    if ts is None:
        return None
    antes = txt(a.get("valor_antes"), 300)
    nuevo = txt(a.get("valor_nuevo"), 300)
    motivo = txt(a.get("motivo"), 400)
    aid = int(a["id"])
    if campo == "serie":
        e = nueva("audit:%d" % aid, "ficha", "serie", "info", "Cambio de N° de serie", ts, False,
                  detalle="%s → %s" % (antes or "(sin serie)", nuevo or "(sin serie)"))
    elif campo == "ubicacion_sala":
        e = nueva("audit:%d" % aid, "ficha", "ubicacion", "info", "Cambio de ubicación", ts, False,
                  detalle="%s → %s" % (antes or "(sin ubicación)", nuevo or "(sin ubicación)"))
    elif campo in AUDIT_DATO_TEXTO:
        e = nueva("audit:%d" % aid, "ficha", "dato", "info", "Se corrigió %s del equipo" % AUDIT_DATO_TEXTO[campo], ts, False,
                  detalle="%s → %s" % (antes or "(vacío)", nuevo or "(vacío)"))
    elif campo == "estado":
        if nuevo == "baja":
            e = nueva("audit:%d" % aid, "estado", "baja", "falla", "Dado de baja desde la ficha", ts, False, detalle=motivo)
            e["_baja"] = True
            e["estado_txt"], e["estado_color"] = "Dado de baja", "negro"
        elif antes == "baja":
            e = nueva("audit:%d" % aid, "estado", "restaurado", "ok", "Restaurado: vuelve a activo", ts, False, detalle=motivo)
        else:
            e = nueva("audit:%d" % aid, "estado", "cambio", "info", "Estado en la ficha: %s → %s" % (
                ESTADO_FICHA_TEXTO.get(antes, antes or "—"), ESTADO_FICHA_TEXTO.get(nuevo, nuevo or "—")), ts, False, detalle=motivo)
    elif campo == "estado_op":
        e = nueva("audit:%d" % aid, "estado", "cambio", "ok" if nuevo == "operativo" else ("aviso" if nuevo else "info"),
                  "Estado operativo: %s → %s" % (ESTADO_OP_TEXTO.get(antes, antes or "—"), ESTADO_OP_TEXTO.get(nuevo, nuevo or "—")), ts, False,
                  detalle=motivo)
    elif campo == "estado_capturado":
        if antes in ESTADOS_PARADA or nuevo in ESTADOS_PARADA:
            return None                                                                     # D7: lo cuenta la parada
        e = nueva("audit:%d" % aid, "estado", "cambio", "ok" if nuevo == "operativo" else "info",
                  "Estado del equipo: %s → %s" % (ESTADO_OP_TEXTO.get(antes, antes or "—"), ESTADO_OP_TEXTO.get(nuevo, nuevo or "—")), ts, False)
    else:
        return None
    e["pide_motivo"] = True
    e["por_que"] = motivo if motivo != "(sin motivo registrado)" else ""
    e["actor"] = _actor(rol, a.get("usuario"))
    return e


def tarjeta_ticket(t, rol):
    """Un ticket de servicio ligado a este equipo (solo gestión)."""
    tid = int(t["id"])
    ts, _ = instante(t.get("created_at"))
    if ts is None:
        return None
    estado = (t.get("estado") or "").strip().lower()
    cerrado = estado in TICKET_CERRADOS
    titulo = " · ".join(x for x in (txt(t.get("numero_ticket"), 40), txt(t.get("titulo"), 300)) if x) or "Ticket %d" % tid
    e = nueva("ticket:%d" % tid, "ticket", (t.get("tipo") or "ticket"), "info" if cerrado else "aviso", titulo, ts, False)
    e["abierto"] = not cerrado
    e["fin_ts"] = t.get("cerrado_at") if (cerrado and isinstance(t.get("cerrado_at"), _dt.datetime)) else None
    e["estado_txt"] = TICKET_ESTADO_TEXTO.get(estado, estado.replace("_", " ").capitalize() or "—")
    e["estado_color"] = "gris" if cerrado else "ambar"
    if (t.get("prioridad") or "") in ("alta", "urgente"):
        _linea(e, "bi-exclamation-triangle-fill", "Prioridad %s" % t["prioridad"])
    e["ref"] = {"tabla": "tk_tickets", "id": tid, "url": "/tickets/%d" % tid, "numero": txt(t.get("numero_ticket"), 40), "texto": "Ver ticket"}
    e["actor"] = _actor(rol, t.get("created_by"))
    return e


def tarjeta_garantia(eq, hoy):
    """La garantía vigente o vencida del equipo (fecha_fin_garantia). None si no hay fecha."""
    fin = eq.get("fecha_fin_garantia")
    if isinstance(fin, _dt.datetime):
        fin = fin.date()
    if not isinstance(fin, _dt.date):
        return None
    ts, _ = instante(fin)
    dias = (fin - hoy).days if isinstance(hoy, _dt.date) else None
    origen = (eq.get("garantia_origen") or "").strip().lower()
    if dias is not None and dias >= 0:
        titulo = "La garantía vence el %s" % fin.strftime("%d/%m/%Y")
        sev = "aviso" if dias <= 30 else "ok"
        estado = ("Vence hoy" if dias == 0 else "Vence en %d día%s" % (dias, "" if dias == 1 else "s")) if dias <= 30 else "Vigente"
    else:
        titulo = "Terminó la garantía el %s" % fin.strftime("%d/%m/%Y")
        sev = "info"
        estado = "Vencida"
    e = nueva("garantia", "garantia", "garantia", sev, titulo, ts, True)
    e["estado_txt"], e["estado_color"] = estado, {"ok": "verde", "aviso": "ambar", "info": "gris"}[sev]
    if origen == "calculada":
        _linea(e, "bi-calculator", "Calculada por el sistema desde la fecha de instalación o de la factura")
    elif origen == "manual":
        _linea(e, "bi-pencil-square", "Fecha puesta a mano en la ficha")
    else:
        _linea(e, "bi-pencil-square", "Fecha registrada en la ficha")
    return e


def tarjeta_alta(eq, rol):
    """El día en que el equipo entró a la ficha del cliente (mant_maquinas.created_at)."""
    ts, _ = instante(eq.get("created_at"))
    if ts is None:
        return None
    e = nueva("alta", "ficha", "alta", "info", "El equipo entró a la ficha del cliente", ts, False)
    if not rol.es_tecnico:
        if txt(eq.get("doc_origen")):
            _linea(e, "bi-file-earmark-text", "Documento de origen: %s" % txt(eq["doc_origen"], 120))
        e["actor"] = _actor(rol, eq.get("created_by"))
    return e


def tarjeta_instalacion(eq):
    """La fecha de instalación que dice la ficha, cuando NO hay una OT de instalación que la cuente."""
    ts, solo = instante(eq.get("fecha_instalacion"))
    if ts is None:
        return None
    return nueva("instalacion", "visita", "instalacion", "info", "Instalación del equipo", ts, True,
                 detalle="Fecha de instalación registrada en la ficha.")


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Qué OT hay que conocer (para sus números y para decidir qué enlaces puede abrir el rol)
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def ids_ot_referenciados(f):
    """Todas las OT que mencionan las filas (visitas, solicitudes, paradas, hallazgos, eventos, diagnósticos, tareas): el servidor
    pregunta por sus números (y por cuáles puede abrir el rol) en UNA consulta."""
    ids = set()

    def sumar(x):
        n = entero(x)
        if n:
            ids.add(n)

    for v in f.get("visitas") or []:
        sumar(v.get("id"))
        sumar(v.get("ot_madre_id"))
    for r in f.get("visita_equipos") or []:
        sumar(r.get("visita_id"))
    for r in f.get("tareas") or []:
        sumar(r.get("visita_id"))
    for r in f.get("solicitudes") or []:
        sumar(r.get("visita_id"))
        sumar(r.get("ot_generada_id"))
    for r in f.get("paradas") or []:
        if r.get("ref_tabla") == "mant_visitas":
            sumar(r.get("ref_id"))
        if r.get("ref_fin_tabla") == "mant_visitas":
            sumar(r.get("ref_fin_id"))
    for r in f.get("hallazgos") or []:
        sumar(r.get("visita_id"))
        sumar(r.get("ot_seguimiento_id"))
    for r in f.get("eventos") or []:
        if r.get("referencia_tabla") == "mant_visitas":
            sumar(r.get("referencia_id"))
    return ids


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  La unificación
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _cerca(a, b, horas=36):
    return isinstance(a, _dt.datetime) and isinstance(b, _dt.datetime) and abs((a - b).total_seconds()) <= horas * 3600


def _clave_orden(e):
    return (e["ts"], -ORDEN_TIPOS.index(e["tipo"]) if e["tipo"] in ORDEN_TIPOS else 0, e["id"])


def _anotar(destino, hallazgo):
    """D5: el hallazgo no se repite; su estado de gestión y sus fotos van en la tarjeta que ya cuenta el hecho."""
    destino["gestion"] = gestion_de(hallazgo)
    for u in hallazgo["_fotos"]:
        if u not in destino["_fotos"]:
            destino["_fotos"].append(u)
    if hallazgo.get("riesgo"):
        destino["riesgo"] = True


def construir(f, rol):
    """Une TODAS las fuentes en la línea de tiempo del equipo, sin duplicar, del más nuevo al más antiguo y ya recortada para el rol.

    `f` (lo arma app.py con consultas de solo lectura): equipo, visitas, visita_equipos, tareas, hallazgos, solicitudes, paradas, eventos,
    auditoria, tickets, lev_visitas (ids de las visitas donde se levantó la ficha), lev_visita ({id de mant_levantamientos: visita_id}),
    numeros_ot ({id: número}), visitas_visibles (set de ids o None), hoy (date de Chile) y ahora (datetime UTC).
    """
    eq = f.get("equipo") or {}
    numeros = f.get("numeros_ot") or {}
    visibles = f.get("visitas_visibles")
    ahora = f.get("ahora") if isinstance(f.get("ahora"), _dt.datetime) else _dt.datetime.utcnow()
    hoy = f.get("hoy") if isinstance(f.get("hoy"), _dt.date) else a_chile(ahora).date()

    out = []                                  # todas las tarjetas, ya deduplicadas

    # ── 1. Diagnósticos por equipo: cuáles tienen tarjeta propia lo decide diagnostico_genera_tarjeta (D8) ─────────────────
    diag_filas = {int(r["visita_id"]): r for r in (f.get("visita_equipos") or []) if r.get("visita_id")}
    diags = {}
    for vid, r in diag_filas.items():
        if diagnostico_genera_tarjeta(r):
            t = tarjeta_diagnostico(r, rol, numeros, visibles)
            if t:
                diags[vid] = t

    # ── 2. Visitas ───────────────────────────────────────────────────────────────────────────────────────────────────────
    lev_visitas = {int(x) for x in (f.get("lev_visitas") or []) if x}
    por_visita = {}
    for v in f.get("visitas") or []:
        vid = int(v["id"])
        fila = dict(v)
        if vid in lev_visitas:
            fila["_levantamiento"] = True
        t = tarjeta_visita(fila, rol, numeros, visibles, rev=(diag_filas.get(vid) if vid not in diags else None), ahora=ahora)
        if t:
            por_visita[vid] = t
            out.append(t)

    # ── 3. Paradas (fuera de servicio / baja) ────────────────────────────────────────────────────────────────────────────
    paradas = [t for t in (tarjeta_parada(p, rol, numeros, visibles, ahora=ahora) for p in (f.get("paradas") or [])) if t]
    out.extend(paradas)

    # ── 4. Solicitudes de repuesto ───────────────────────────────────────────────────────────────────────────────────────
    solicitudes = [t for t in (tarjeta_solicitud(s, rol, numeros, visibles) for s in (f.get("solicitudes") or [])) if t]
    out.extend(solicitudes)
    sol_ids = {t["_solicitud_id"] for t in solicitudes}

    # ── 5. Eventos de la hoja de vida: solo los que nadie más cuenta (D1, D2) ────────────────────────────────────────────
    lev_a_visita = {int(k): int(v) for k, v in (f.get("lev_visita") or {}).items() if k and v}
    bajas_visitas = {vid for p in paradas if p["_baja"] for vid in p["_parada_visitas"]}
    for ev in f.get("eventos") or []:
        clave = (ev.get("tipo") or "").strip().lower()
        if clave not in _EVENTO_TIPO:
            continue                                                                        # D2: fotos y «otro» son ruido
        if clave in ("cambio_estado", "dado_baja") and _json_dict(ev.get("metadata_json")).get("parada"):
            continue                                                                        # D2: la parada lo cuenta con su hora real
        ref_t, ref_i = ev.get("referencia_tabla"), entero(ev.get("referencia_id"))
        if ref_t == "mant_visitas" and ref_i in por_visita:
            continue                                                                        # D1: la visita lo cuenta
        if ref_t == "mant_levantamientos" and lev_a_visita.get(ref_i) in por_visita:
            continue                                                                        # D1: el levantamiento es la visita
        if ref_t == "mant_ot_repuesto_solicitudes" and ref_i in sol_ids:
            continue                                                                        # D1: la solicitud lo cuenta
        if clave == "dado_baja" and ref_t == "mant_visitas" and ref_i in bajas_visitas:
            continue                                                                        # la baja de esa visita ya está
        t = tarjeta_evento(ev, rol, numeros, visibles)
        if t:
            out.append(t)

    # ── 6. Diagnósticos: D6 ──────────────────────────────────────────────────────────────────────────────────────────────
    visitas_con_parada = {vid for p in paradas if not p["_baja"] for vid in p["_parada_visitas"]}
    bajas_visitas |= {t["_visita_id"] for t in out if t["_baja"] and t["_visita_id"]}
    diag_en_out = {}
    for vid in sorted(diags):
        t = diags[vid]
        if t["_razon"] == "fuera_de_servicio" and vid in visitas_con_parada:
            continue                                                                        # D6: la parada lo cuenta
        if t["_razon"] == "dado_de_baja":
            if vid in bajas_visitas:
                continue                                                                    # D6: la baja de esa visita ya está
            t["tipo"], t["subtipo"], t["severidad"] = "estado", "baja", "falla"            # D6: la baja que nadie más contó
            t["titulo"] = "Dado de baja"
            t["detalle"] = re.sub(r"^Baja:\s*", "", t["detalle"])
            t["estado_txt"], t["estado_color"] = "Dado de baja", "negro"
            t["_baja"] = True
            t["id"] = "baja:%d" % vid
        out.append(t)
        diag_en_out[vid] = t

    # ── 7. Fallas y alertas del checklist ────────────────────────────────────────────────────────────────────────────────
    checklist = {}
    for r in f.get("tareas") or []:
        ev = _oh.evaluar_tarea(r)
        if not ev:
            continue
        vt = por_visita.get(int(r["visita_id"]))
        t = tarjeta_checklist(r, ev, vt["ts"] if vt else None, rol, numeros, visibles)
        if t:
            checklist[t["_tarea_id"]] = t

    # ── 8. Hallazgos (solo gestión): D3, D4, D5 ──────────────────────────────────────────────────────────────────────────
    hall = []
    if rol.gestion:
        for h in f.get("hallazgos") or []:
            if h.get("retirado_en"):
                continue                                                                    # el técnico lo retiró: ya no es un hecho
            t = tarjeta_hallazgo(h, rol, numeros, visibles)
            if t:
                hall.append(t)
    for t in sorted(hall, key=lambda x: x["ts"]):
        sev, vid = t["_hallazgo_sev"], t["_visita_id"]
        if t["_tarea_id"] and t["_tarea_id"] in checklist:                                  # D3
            base = checklist.pop(t["_tarea_id"])
            if not t["detalle"]:
                t["detalle"] = base["detalle"]
            out.append(t)
            continue
        if sev == _oh.SEV_FALLA and t["_referencia"].endswith(":falla") and vid in diag_en_out and _esta(out, diag_en_out[vid]):
            base = diag_en_out[vid]                                                         # D4
            _quitar(out, base)
            if not t["detalle"]:
                t["detalle"] = base["detalle"]
            out.append(t)
            continue
        if sev == _oh.SEV_FUERA_SERVICIO:                                                   # D5
            dest = [p for p in paradas if not p["_baja"] and (vid in p["_parada_visitas"] or _cerca(p["ts"], t["ts"]))]
            if dest:
                _anotar(dest[0], t)
                continue
        elif sev == _oh.SEV_BAJA:
            dest = [b for b in out if b["_baja"] and (b["_visita_id"] == vid or vid in b["_parada_visitas"])]
            if dest:
                _anotar(dest[0], t)
                continue
        elif sev == _oh.SEV_REPUESTO:
            dest = [s for s in solicitudes if s["_visita_id"] == vid]
            if dest:
                for d in dest:
                    _anotar(d, t)
                continue
        out.append(t)
    out.extend(checklist.values())

    # ── 9. Cambios de la ficha (N° de serie, ubicación, datos, estado): D7 ───────────────────────────────────────────────
    for a in f.get("auditoria") or []:
        t = tarjeta_auditoria(a, rol)
        if not t:
            continue
        if t["_baja"] and any(b["_baja"] and _cerca(b["ts"], t["ts"], horas=24) for b in out):
            continue                                                                        # la baja ya está contada: no se repite
        out.append(t)

    # ── 10. Tickets (solo gestión), garantía y los hitos de la ficha ─────────────────────────────────────────────────────
    if rol.gestion:
        out.extend(t for t in (tarjeta_ticket(tk, rol) for tk in (f.get("tickets") or [])) if t)
    g = tarjeta_garantia(eq, hoy)
    if g:
        out.append(g)
    alta = tarjeta_alta(eq, rol)
    if alta:
        out.append(alta)
    if eq.get("fecha_instalacion") and not any(x["tipo"] == "visita" and x["subtipo"] == "instalacion" for x in out):
        inst = tarjeta_instalacion(eq)
        if inst:
            out.append(inst)

    for e in out:
        # POR QUÉ: si el hecho lo exige y nadie escribió un motivo aparte, vale el texto del propio hecho (diagnóstico, observación, motivo del
        # pedido). Si tampoco hay, queda vacío y la pantalla dice «sin motivo registrado»: nunca se inventa.
        if e["pide_motivo"] and not e["por_que"] and e["tipo"] in ("diagnostico", "checklist", "hallazgo", "repuesto"):
            e["por_que"] = e["detalle"]
    _poner_estados_ot(out, f.get("estados_ot"))
    return recortar_para_rol(sorted(out, key=_clave_orden, reverse=True), rol)


def _poner_estados_ot(out, estados):
    """Cada enlace a una OT dice cómo está esa OT (en curso o cerrada...): las OT abiertas y las cerradas se ven, y se distinguen."""
    for e in out:
        for r in ([e["ref"]] if e["ref"] else []) + list(e["refs"]):
            if r.get("tabla") == "mant_visitas":
                est = ((estados or {}).get(r["id"]) or "").strip().lower()
                if est:
                    r["estado"] = "En curso" if est in OT_EN_CURSO else ESTADO_OT.get(est, (est.replace("_", " ").capitalize(),))[0]
                    r["en_curso"] = est in OT_EN_CURSO


def ligada_ot(e):
    """¿Este hecho viene de trabajo en terreno, es decir, está ligado a una OT?"""
    return bool(e.get("ref") and e["ref"].get("tabla") == "mant_visitas")


def filtrar_ot(entradas):
    """«Solo lo ligado a una OT»."""
    return [e for e in entradas if ligada_ot(e)]


def ultimo_gps(visitas, rol, numeros=None):
    """El último GPS de visita (donde el técnico inició la ejecución): solo gestión (es la ubicación de una persona). None si no hay."""
    if rol.es_tecnico:
        return None
    mejor = None
    for v in visitas or []:
        lat, lng = v.get("exec_gps_lat"), v.get("exec_gps_lng")
        if lat is None or lng is None:
            continue
        ts = v.get("exec_gps_at") if isinstance(v.get("exec_gps_at"), _dt.datetime) else instante(v.get("fecha_programada"))[0]
        if ts is None:
            continue
        if mejor is None or ts > mejor[0]:
            mejor = (ts, v, float(lat), float(lng))
    if not mejor:
        return None
    ts, v, lat, lng = mejor
    return {"fecha": fmt_fecha(ts), "direccion": txt(v.get("exec_gps_direccion"), 300), "lat": lat, "lng": lng,
            "url": "https://www.google.com/maps?q=%.6f,%.6f" % (lat, lng), "ot": numero_ot(v["id"], numeros)}


def frase_resumen(enc, entradas, res, ahora, rol=None, gps=None):
    """El resumen de arriba, en tres frases con datos reales: DÓNDE está, QUÉ se le hizo por última vez (y por qué, en qué OT) y cómo está HOY."""
    cliente = enc.get("cliente") or "un cliente sin nombre"
    donde = "Está en %s" % cliente + (" · %s" % enc["ubicacion"] if enc.get("ubicacion") else "")
    if gps:
        donde += ". Último GPS de visita: %s (%s)" % (gps["direccion"] or "sin dirección", gps["fecha"])
    donde += "."
    ult = next((e for e in entradas if e["tipo"] == "visita" and ligada_ot(e) and e["ts"] <= ahora
                and not e["estado_txt"].startswith(("Cancelada", "Anulada"))), None)
    if ult:
        que = ult.get("_label") or "Visita"
        fecha = fmt_fecha(ult["ts"], ult["solo_fecha"])
        porque = ult["por_que"] or "sin motivo registrado"
        ultima = "Última intervención: %s el %s en %s%s porque %s." % (
            que, fecha, ult["ref"]["numero"], " (en curso)" if ult["abierto"] else "", porque.rstrip(". "))
    else:
        ultima = "Todavía no tiene intervenciones registradas en ninguna OT."
    cosas = []
    est = (enc.get("estado") or {}).get("texto", "Operativo").lower()
    fs = (res or {}).get("fuera_servicio")
    cosas.append("fuera de servicio desde %s" % fs["desde"] if fs else est)
    n = (res or {}).get("repuestos_abiertos", 0)
    if n:
        cosas.append("%d pedido%s de repuesto en curso" % (n, "" if n == 1 else "s"))
    n = (res or {}).get("hallazgos_abiertos", 0)
    if n:
        cosas.append("%d hallazgo%s sin cerrar" % (n, "" if n == 1 else "s"))
    n = (res or {}).get("ot_en_curso", 0)
    if n:
        cosas.append("%d OT en curso" % n)
    n = (res or {}).get("tickets_abiertos", 0)
    if n:
        cosas.append("%d ticket%s abierto%s" % (n, "" if n == 1 else "s", "" if n == 1 else "s"))
    if len(cosas) == 1:
        cosas.append("sin pedidos ni OT abiertas" if (rol and rol.es_tecnico) else "sin pedidos, OT ni hallazgos abiertos")
    hoy = "Hoy: %s." % "; ".join(cosas)
    return {"donde": donde, "ultima": ultima, "hoy": hoy, "texto": " ".join((donde, ultima, hoy)), "ot": ult["ref"]["numero"] if ult else None}


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Recorte por rol (REGLAS #19 y #26): segunda barrera. La primera es no consultar ni construir lo prohibido.
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def recortar_para_rol(entradas, rol):
    permitidos = set(tipos_permitidos(rol))
    out = []
    for e in entradas:
        if e["tipo"] not in permitidos:
            continue
        if rol.es_tecnico:
            e["actor"] = None
            e["gestion"] = None
            e["refs"] = [r for r in e["refs"] if r.get("tabla") not in ("mant_ot_hallazgos", "tk_tickets")]
        elif not rol.es_superadmin and e["tipo"] in ("diagnostico", "checklist"):
            e["actor"] = None                                                               # el «quién» del diagnóstico: solo superadmin
        out.append(e)
    return out


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Filtros, contadores, paginación y resumen
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def limpiar_tipos(tipos, rol):
    """['hallazgo', 'x', ...] o 'a,b' → solo claves válidas Y permitidas para el rol, sin repetir. Vacío = todos."""
    if isinstance(tipos, str):
        tipos = [p for p in re.split(r"[\s,;]+", tipos) if p]
    permitidos = set(tipos_permitidos(rol))
    out = []
    for t in tipos or []:
        t = str(t).strip().lower()
        if t in permitidos and t not in out:
            out.append(t)
    return out


def filtrar_fechas(entradas, desde=None, hasta=None):
    """Deja las tarjetas cuyo día (hora Chile) cae entre `desde` y `hasta` (inclusive). Cada límite puede ser None o inválido (se ignora)."""
    d, h = fecha_iso(desde), fecha_iso(hasta)
    if not d and not h:
        return list(entradas)
    out = []
    for e in entradas:
        dia = dia_chile(e["ts"])
        if dia is None or (d and dia < d) or (h and dia > h):
            continue
        out.append(e)
    return out


def filtrar_tipos(entradas, tipos):
    if not tipos:
        return list(entradas)
    s = set(tipos)
    return [e for e in entradas if e["tipo"] in s]


def contar(entradas):
    """{tipo: n} solo con los tipos que tienen algo, más «todos»."""
    c = {}
    for e in entradas:
        c[e["tipo"]] = c.get(e["tipo"], 0) + 1
    c["todos"] = len(entradas)
    c["ot"] = sum(1 for e in entradas if ligada_ot(e))
    return c


def normalizar_pagina(pagina, por_pagina):
    try:
        pp = int(por_pagina)
    except (TypeError, ValueError):
        pp = POR_PAGINA_DEFECTO
    if pp not in POR_PAGINA_OPCIONES:
        pp = POR_PAGINA_DEFECTO
    try:
        p = int(pagina)
    except (TypeError, ValueError):
        p = 1
    return max(1, p), pp


def paginar(entradas, pagina, por_pagina):
    """→ (tarjetas de la página, meta). La página se ajusta si se pasa del final. «Mostrando A–B de N» como en Etiquetas (REGLA #4.3)."""
    p, pp = normalizar_pagina(pagina, por_pagina)
    total = len(entradas)
    paginas = max(1, (total + pp - 1) // pp)
    p = min(p, paginas)
    ini = (p - 1) * pp
    return entradas[ini:ini + pp], {
        "total": total, "pagina": p, "paginas": paginas, "por_pagina": pp,
        "desde": (ini + 1) if total else 0, "hasta": min(ini + pp, total),
    }


def resumen(entradas, ahora):
    """Lo que está pendiente HOY (encabezado de la pantalla): fuera de servicio, hallazgos sin gestionar, repuestos en curso, tickets."""
    r = {"fuera_servicio": None, "hallazgos_abiertos": 0, "repuestos_abiertos": 0, "tickets_abiertos": 0, "ultima_visita": None,
         "n_visitas": 0, "ot_en_curso": 0}
    for e in entradas:
        if e["tipo"] == "estado" and e["abierto"] and not e["_baja"]:
            if r["fuera_servicio"] is None:
                r["fuera_servicio"] = {"desde": fmt_fecha(e["ts"]), "titulo": e["titulo"], "riesgo": e["riesgo"], "hace": hace_texto(e["ts"], ahora)}
        elif e["tipo"] == "hallazgo" and e["abierto"]:
            r["hallazgos_abiertos"] += 1
        elif e["tipo"] == "repuesto" and e["abierto"]:
            r["repuestos_abiertos"] += 1
        elif e["tipo"] == "ticket" and e["abierto"]:
            r["tickets_abiertos"] += 1
        elif e["tipo"] == "visita" and e["subtipo"] != "instalacion":
            r["n_visitas"] += 1
            if e["abierto"]:
                r["ot_en_curso"] += 1
            if r["ultima_visita"] is None and e["ts"] <= ahora and e["severidad"] != "info":
                r["ultima_visita"] = {"fecha": fmt_fecha(e["ts"], e["solo_fecha"]), "titulo": e["titulo"]}
    return r


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Salida al navegador
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def url_segura(u):
    """Solo rutas internas («/…») o https: nada de javascript: ni esquemas raros en un href/src."""
    u = str(u or "").strip()
    return u if (u.startswith("/") and not u.startswith("//")) or u.startswith("https://") else ""


def a_json(e, fotos_extra=None, ahora=None):
    """La tarjeta tal como viaja al navegador: sin claves internas, con las fechas ya en hora Chile (REGLA #6)."""
    fotos = []
    for u in list(e.get("_fotos") or []) + list(fotos_extra or []):
        u = url_segura(u)
        if u and u not in fotos:
            fotos.append(u)
    t = TIPOS[e["tipo"]]
    return {
        "id": e["id"], "tipo": e["tipo"], "tipo_texto": t["texto"], "subtipo": e["subtipo"], "icono": t["icono"],
        "severidad": e["severidad"], "titulo": e["titulo"], "detalle": e["detalle"],
        "fecha": fmt_fecha(e["ts"], e["solo_fecha"]), "mes": mes_clave(e["ts"]), "mes_txt": mes_texto(e["ts"]),
        "futuro": bool(isinstance(ahora, _dt.datetime) and e["ts"] > ahora),
        "fecha_fin": fmt_fecha(e["fin_ts"], e["fin_solo_fecha"]) if e.get("fin_ts") else "",
        "abierto": bool(e["abierto"]), "estado_txt": e["estado_txt"], "estado_color": e["estado_color"],
        "riesgo": bool(e["riesgo"]), "por_que": e["por_que"], "pide_motivo": bool(e["pide_motivo"]), "ligada_ot": ligada_ot(e),
        "lineas": [dict(x) for x in e["lineas"]], "pasos": [dict(x) for x in e["pasos"]],
        "ref": dict(e["ref"]) if e.get("ref") else None, "refs": [dict(r) for r in e["refs"]],
        "actor": e["actor"], "gestion": dict(e["gestion"]) if e.get("gestion") else None, "fotos": fotos[:FOTOS_MAX],
    }


def encabezado(eq, hoy):
    """La cabecera de la pantalla: nombre completo, SKU, N° de serie, cliente, estado actual y garantía."""
    estado_cap = (eq.get("estado_capturado") or "").strip().lower()
    estado_fi = (eq.get("estado") or "").strip().lower()
    if estado_fi == "baja" or estado_cap == "dado_baja":
        est = {"clave": "baja", "texto": "Dado de baja", "color": "negro"}
    elif estado_cap in ESTADOS_PARADA:
        est = {"clave": estado_cap, "texto": ESTADO_OP_TEXTO.get(estado_cap, "Fuera de servicio"), "color": "rojo"}
    elif estado_cap == "advertencia":
        est = {"clave": "advertencia", "texto": "Con advertencia", "color": "ambar"}
    else:
        est = {"clave": "operativo", "texto": "Operativo", "color": "verde"}
    fin = eq.get("fecha_fin_garantia")
    if isinstance(fin, _dt.datetime):
        fin = fin.date()
    if isinstance(fin, _dt.date):
        dias = (fin - hoy).days if isinstance(hoy, _dt.date) else None
        if dias is not None and dias >= 0:
            gar = {"texto": "Vigente hasta el %s" % fin.strftime("%d/%m/%Y"), "color": "ambar" if dias <= 30 else "verde"}
        else:
            gar = {"texto": "Venció el %s" % fin.strftime("%d/%m/%Y"), "color": "gris"}
    else:
        gar = {"texto": "Sin garantía registrada", "color": "gris"}
    return {
        "id": entero(eq.get("id")), "nombre": txt(eq.get("nombre"), 400) or "Equipo #%s" % eq.get("id"),
        "sku": txt(eq.get("sku"), 100), "serie": txt(eq.get("serie"), 100),
        "marca": txt(eq.get("marca"), 120), "modelo": txt(eq.get("modelo"), 120),
        "cliente": txt(eq.get("razon_social"), 300), "cliente_id": entero(eq.get("cliente_id")),
        "ubicacion": txt(eq.get("ubicacion_sala"), 200),
        "estado": est, "garantia": gar,
    }

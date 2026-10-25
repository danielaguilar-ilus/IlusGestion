# -*- coding: utf-8 -*-
"""🧭 Intenciones (motivos) del costeo de OT: POR QUÉ una OT no se cobra, se cierra sin factura/boleta, se cobra distinto del documento o
va a pérdida, y QUIÉN lo asume. LÓGICA PURA (sin Flask ni base de datos), con pruebas en tests/test_ot_intenciones.py.

Daniel (2026-10-25): «quería agregar los motivos, pero solamente para ser administrador: garantía, regalía, arriendo, leasing, evento,
marketing, el precio lo asume comercial… algo dinámico para la toma de decisiones en el mes» y «todo cobro, sea instalación o mantención
o visita, sea gestionado con NVV, BLV y FCV, pero sea CERRADO con FCV o BLV; de lo contrario bloquea la OT solicitando autorización, ya
que puede ser arriendo, por descuadre de precio o pérdida, puede ser costo asumido por marketing… ¿Quién lo asume? Si es comercial. ¿Por
qué? Autorizaron. Toda esa trazabilidad la necesito».

Qué es cada cosa:
  · La FAMILIA contable de un $0 (`cobro_cero_motivo`: garantia | regalia | arriendo_leasing) NO cambia: de ella dependen la modalidad de cobro,
    la puerta del documento y todo el costeo. «Garantía» no se renombra (no afectar OT ya hechas).
  · La INTENCIÓN es la razón de negocio, más fina y administrable: una fila del catálogo `ot_intenciones` (clave, nombre, a qué familia
    pertenece si es un $0, en qué situaciones se puede elegir, qué centro de costo la asume y si solo la puede elegir un administrador).
  · Cada solicitud de autorización (cerrar sin factura/boleta, cerrar con pérdida, cobrar sobre el saldo, $0, crear sin documento, documento
    observado) lleva SU intención, el argumento de quien la pide («mensaje del usuario») y un «mensaje del sistema» que dice qué pasó.
El sistema SUGIERE (con su origen a la vista) y la persona DECIDE (REGLA #32); nada se borra: una intención se desactiva.
"""
import re
import unicodedata

# ───────────────────────── en qué situaciones se elige una intención ─────────────────────────
USO_COBERTURA = "cobertura"            # un $0 (garantía, regalía, arriendo/leasing…): también la autorización 'cobro_cero'
USOS = ("cobertura", "cerrar_sin_documento", "cerrar_con_perdida", "exceder_saldo", "crear_sin_documento", "documento_observado")
USOS_TXT = {
    "cobertura": "Un $0 (la OT no se cobra)",
    "cerrar_sin_documento": "Cerrar sin factura ni boleta",
    "cerrar_con_perdida": "Cerrar con pérdida",
    "exceder_saldo": "Cobrar distinto del documento (descuadre de precio)",
    "crear_sin_documento": "Crear sin documento todavía",
    "documento_observado": "Crear con un documento observado",
}
FAMILIAS_CERO = ("garantia", "regalia", "arriendo_leasing")
FAMILIAS_TXT = {"garantia": "Garantía", "regalia": "Regalía", "arriendo_leasing": "Arriendo o leasing"}
# El tipo de autorización (mant_ot_autorizaciones.tipo) → la situación que clasifica. «desinstalacion_cubierta» no se clasifica.
_TIPO_A_USO = {"cobro_cero": "cobertura", "cerrar_sin_documento": "cerrar_sin_documento", "cerrar_con_perdida": "cerrar_con_perdida",
               "exceder_saldo": "exceder_saldo", "crear_sin_documento": "crear_sin_documento", "documento_observado": "documento_observado"}
CLAVE_OTRO = "otro"
CLAVE_RE = re.compile(r"^[a-z][a-z0-9_]{1,38}$")
LARGO_NOMBRE_MIN, LARGO_NOMBRE_MAX, LARGO_DESCRIPCION_MAX = 3, 80, 300

# (clave, nombre, descripción, familia_cero, aplica_a, asume_centro, solo_admin, sistema, orden). Se siembra con INSERT IGNORE por clave:
# lo que Daniel edite después NUNCA se pisa. `solo_admin` arranca apagado en todas (se enciende por intención desde la pantalla).
SEMILLA = (
    ("garantia", "Garantía", "El equipo está cubierto por la garantía.", "garantia",
     ("cobertura", "cerrar_sin_documento", "cerrar_con_perdida"), "sstt", False, True, 10),
    ("regalia", "Regalía", "Cortesía comercial: se entrega el servicio sin cobrarlo.", "regalia",
     ("cobertura", "cerrar_sin_documento"), "comercial", False, True, 20),
    ("arriendo_leasing", "Arriendo o leasing", "El servicio va incluido en el arriendo o leasing (se factura aparte, mes a mes).",
     "arriendo_leasing", ("cobertura", "cerrar_sin_documento"), None, False, True, 30),
    ("evento", "Evento", "Servicio para un evento de la marca: no se cobra.", "regalia",
     ("cobertura", "cerrar_sin_documento", "cerrar_con_perdida"), "marketing", False, False, 40),
    ("marketing", "Costo asumido por Marketing", "Marketing se hace cargo de lo que cuesta esta OT.", "regalia",
     ("cobertura", "cerrar_sin_documento", "cerrar_con_perdida", "exceder_saldo"), "marketing", False, False, 50),
    ("comercial_asume", "Costo asumido en el precio de Comercial", "El precio ya lo absorbió el área comercial al vender.", "regalia",
     ("cobertura", "cerrar_sin_documento", "cerrar_con_perdida", "exceder_saldo"), "comercial", False, False, 60),
    ("descuadre_precio", "Descuadre de precio", "Lo cobrado no calza con lo que dice el documento o la cotización.", None,
     ("cerrar_sin_documento", "cerrar_con_perdida", "exceder_saldo", "crear_sin_documento", "documento_observado"), "comercial", False, False, 70),
    ("mal_cobro", "Mal cobro en los servicios", "Se cobró de menos o mal el servicio y no alcanza a cubrir el costo.", None,
     ("cerrar_con_perdida", "exceder_saldo"), "comercial", False, False, 80),
    ("perdida_asumida", "Pérdida asumida", "Se decide absorber la pérdida de esta OT.", None,
     ("cerrar_con_perdida",), None, False, False, 90),
    ("documento_en_tramite", "Documento en trámite", "La factura o boleta todavía no se emite; se regulariza después.", None,
     ("cerrar_sin_documento", "crear_sin_documento", "documento_observado"), None, False, False, 100),
    (CLAVE_OTRO, "Otro motivo", "Ninguno de los anteriores: el argumento debe explicarlo con detalle.", None,
     USOS, None, False, True, 990),
)

# Qué intención se sugiere según el centro de costo que asume la OT (y, si hace falta, el tipo de OT), por orden de preferencia.
_PREFERENCIA_POR_CENTRO = {"marketing": ("marketing", "evento"), "comercial": ("comercial_asume",), "sstt": ("garantia",)}
_TIPOS_SSTT_GARANTIA = ("mantencion", "correctiva", "reparacion", "revision", "levantamiento")


# ───────────────────────── el catálogo ─────────────────────────
def _txt(v, largo):
    return str(v or "").strip()[:largo]


def _bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "si", "sí", "yes", "on")
    return bool(v)


def _lista_usos(v):
    if isinstance(v, str):
        v = [p for p in re.split(r"[,\s]+", v) if p]
    out = []
    for u in (v or []):
        u = str(u).strip().lower()
        if u in USOS and u not in out:
            out.append(u)
    return out


def fila_de(raw):
    """Una fila del catálogo (de la base o de la SEMILLA) en su forma normal: {clave, nombre, descripcion, familia_cero, aplica_a [lista], asume_centro,
    solo_admin, activa, sistema, orden}. None si no trae clave."""
    if not isinstance(raw, dict):
        return None
    clave = str(raw.get("clave") or "").strip().lower()
    if not clave:
        return None
    fam = str(raw.get("familia_cero") or "").strip().lower()
    try:
        orden = int(raw.get("orden") if raw.get("orden") is not None else 500)
    except (TypeError, ValueError):
        orden = 500
    return {"clave": clave, "nombre": _txt(raw.get("nombre"), LARGO_NOMBRE_MAX) or clave,
            "descripcion": _txt(raw.get("descripcion"), LARGO_DESCRIPCION_MAX),
            "familia_cero": fam if fam in FAMILIAS_CERO else None,
            "aplica_a": _lista_usos(raw.get("aplica_a")), "asume_centro": (str(raw.get("asume_centro") or "").strip().lower() or None),
            "solo_admin": _bool(raw.get("solo_admin")), "activa": _bool(raw.get("activa", True)),
            "sistema": _bool(raw.get("sistema")), "orden": orden}


def catalogo_de(filas):
    """El catálogo ordenado (orden, nombre) a partir de las filas crudas de la base."""
    out = [f for f in (fila_de(r) for r in (filas or [])) if f]
    out.sort(key=lambda f: (f["orden"], f["nombre"].lower()))
    return out


def semilla_filas():
    """Las filas de la SEMILLA como dicts (para el INSERT IGNORE y como respaldo si la tabla aún no existe)."""
    return [{"clave": c, "nombre": n, "descripcion": d, "familia_cero": f, "aplica_a": ",".join(a), "asume_centro": ac,
             "solo_admin": int(sa), "activa": 1, "sistema": int(si), "orden": o}
            for (c, n, d, f, a, ac, sa, si, o) in SEMILLA]


def catalogo_semilla():
    return catalogo_de(semilla_filas())


def por_clave(catalogo=None):
    """{clave: fila}. Sin catálogo, el de la SEMILLA."""
    return {f["clave"]: f for f in (catalogo if catalogo is not None else catalogo_semilla())}


def intenciones_por_clave(catalogo=None):
    """Función pública para otras pantallas (p. ej. el dashboard gerencial): {clave: {nombre, familia_cero, asume_centro, …}}."""
    return por_clave(catalogo)


def uso_de(tipo_autorizacion):
    """El «uso» del catálogo que clasifica un tipo de autorización (None si ese tipo no se clasifica)."""
    return _TIPO_A_USO.get((tipo_autorizacion or "").strip().lower())


def nombre_de(clave, catalogo=None):
    f = por_clave(catalogo).get((clave or "").strip().lower())
    return f["nombre"] if f else ""


def visibles(catalogo, uso, es_admin, incluir=None):
    """Lo que se le OFRECE a quien pide: activas, que aplican a `uso`, y las `solo_admin` solo a un administrador. `incluir` = una clave que se
    muestra aunque ya no se pueda elegir (para repintar una selección anterior)."""
    out = []
    for f in (catalogo or []):
        if incluir and f["clave"] == incluir:
            out.append(f)
            continue
        if not f["activa"] or uso not in f["aplica_a"]:
            continue
        if f["solo_admin"] and not es_admin:
            continue
        out.append(f)
    return out


def validar(clave, catalogo, uso, es_admin, requerida=True):
    """La validación que repite el SERVIDOR de la intención que eligió la persona. Devuelve (error|None, fila|None); `error` es
    {error, error_codigo}. Sin clave: error INTENCION_REQUERIDA si `requerida`; si no, (None, None)."""
    clave = (clave or "").strip().lower()
    if not clave:
        if requerida:
            return {"error": "Elige «¿Por qué?»: es la clasificación que queda guardada junto a la autorización.",
                    "error_codigo": "INTENCION_REQUERIDA"}, None
        return None, None
    f = por_clave(catalogo).get(clave)
    if not f:
        return {"error": "Esa clasificación no existe.", "error_codigo": "INTENCION_INVALIDA"}, None
    if not f["activa"]:
        return {"error": f"«{f['nombre']}» está desactivada: elige otra.", "error_codigo": "INTENCION_INACTIVA"}, None
    if uso not in f["aplica_a"]:
        return {"error": f"«{f['nombre']}» no se usa para «{USOS_TXT.get(uso, uso)}»: elige otra.", "error_codigo": "INTENCION_NO_APLICA"}, None
    if f["solo_admin"] and not es_admin:
        return {"error": f"«{f['nombre']}» solo la puede elegir un administrador: elige otra o pídesela a uno.",
                "error_codigo": "INTENCION_SOLO_ADMIN"}, None
    return None, f


def familia_de(clave, catalogo=None):
    """La familia contable de un $0 para esa intención (la propia clave si es una de las tres base). None si no es una cobertura."""
    clave = (clave or "").strip().lower()
    if clave in FAMILIAS_CERO:
        return clave
    f = por_clave(catalogo).get(clave)
    return f["familia_cero"] if f else None


# ───────────────────────── sugerencia por contexto (el sistema sugiere, la persona decide) ─────────────────────────
def sugerir(catalogo, uso, centro, tipo_ot, es_admin):
    """La intención que se SUGIERE y de dónde sale, o None. Devuelve {clave, nombre, origen}. Solo sugiere lo que la persona podría elegir
    (activa, aplicable y permitida). Nunca obliga: la pantalla la deja elegida pero editable, con su origen a la vista."""
    centro = (centro or "").strip().lower()
    tipo_ot = (tipo_ot or "").strip().lower()
    if not centro:
        return None
    ofrecidas = {f["clave"]: f for f in visibles(catalogo, uso, es_admin)}
    prefer = list(_PREFERENCIA_POR_CENTRO.get(centro, ()))
    if centro == "sstt" and tipo_ot and tipo_ot not in _TIPOS_SSTT_GARANTIA:
        prefer = []          # una instalación de Servicio Técnico no se presume garantía
    for clave in prefer:
        if clave in ofrecidas:
            f = ofrecidas[clave]
            return {"clave": clave, "nombre": f["nombre"], "origen": f"Sugerida según su centro de costo ({_centro_txt(centro)})"}
    return None


def _centro_txt(centro):
    return {"sstt": "Servicio Técnico", "logistica": "Logística", "comercial": "Comercial", "marketing": "Marketing"}.get(centro, centro or "")


# ───────────────────────── gestión del catálogo (solo superadmin) ─────────────────────────
def slug(nombre):
    t = unicodedata.normalize("NFD", str(nombre or ""))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn").lower()
    t = re.sub(r"[^a-z0-9]+", "_", t).strip("_")
    if t and not t[0].isalpha():
        t = "i_" + t
    return t[:40]


def validar_edicion(datos, existente, claves_ocupadas, centros_validos):
    """Valida lo que manda la pantalla de gestión. `existente` = la fila actual (None si se crea). Devuelve (error|None, limpio): `limpio` trae lo que se
    escribe ({clave, nombre, descripcion, familia_cero, aplica_a, asume_centro, solo_admin, orden}). Las de sistema conservan clave y familia, nunca
    se desactivan (ni «Otro motivo» deja de aplicar a todo, ni las tres base dejan de ser coberturas)."""
    d = datos if isinstance(datos, dict) else {}
    nombre = _txt(d.get("nombre"), LARGO_NOMBRE_MAX)
    if len(nombre) < LARGO_NOMBRE_MIN:
        return {"error": f"Escribe el nombre (mínimo {LARGO_NOMBRE_MIN} letras).", "error_codigo": "NOMBRE_CORTO"}, None
    aplica = _lista_usos(d.get("aplica_a"))
    if not aplica:
        return {"error": "Elige al menos una situación donde se pueda usar.", "error_codigo": "USO_REQUERIDO"}, None
    fam = str(d.get("familia_cero") or "").strip().lower() or None
    if fam is not None and fam not in FAMILIAS_CERO:
        return {"error": "La familia del $0 solo puede ser Garantía, Regalía o Arriendo/leasing.", "error_codigo": "FAMILIA_INVALIDA"}, None
    if fam and "cobertura" not in aplica:
        return {"error": "Si pertenece a una familia de $0, tiene que poder usarse en «Un $0».", "error_codigo": "FAMILIA_SIN_COBERTURA"}, None
    if "cobertura" in aplica and not fam:
        fam = "regalia"      # un $0 siempre cae en una familia contable; si no se dijo, es regalía (nunca toca una garantía)
    centro = str(d.get("asume_centro") or "").strip().lower() or None
    if centro and centro not in set(centros_validos or ()):
        return {"error": "Ese centro de costo no existe.", "error_codigo": "CENTRO_INVALIDO"}, None
    try:
        orden = max(0, min(999, int(d.get("orden") if d.get("orden") not in (None, "") else (existente or {}).get("orden", 500))))
    except (TypeError, ValueError):
        orden = 500
    if existente:
        clave = existente["clave"]
        if existente.get("sistema"):
            fam = existente.get("familia_cero")
            if clave in FAMILIAS_CERO and "cobertura" not in aplica:
                return {"error": f"«{existente['nombre']}» es una de las tres familias del $0: tiene que seguir usándose en «Un $0».",
                        "error_codigo": "SISTEMA_COBERTURA"}, None
            if clave == CLAVE_OTRO:
                aplica = list(USOS)
    else:
        clave = slug(d.get("clave") or nombre)
        if not CLAVE_RE.match(clave):
            return {"error": "No se pudo armar la clave con ese nombre: usa letras y números.", "error_codigo": "CLAVE_INVALIDA"}, None
        if clave in set(claves_ocupadas or ()):
            return {"error": "Ya existe una clasificación con ese nombre.", "error_codigo": "CLAVE_REPETIDA"}, None
    return None, {"clave": clave, "nombre": nombre, "descripcion": _txt(d.get("descripcion"), LARGO_DESCRIPCION_MAX), "familia_cero": fam,
                  "aplica_a": aplica, "asume_centro": centro, "solo_admin": _bool(d.get("solo_admin")), "orden": orden}


def puede_desactivar(fila):
    """(ok, motivo): las de sistema (las tres familias del $0 y «Otro motivo») no se desactivan."""
    if fila and fila.get("sistema"):
        return False, f"«{fila['nombre']}» es parte del sistema: no se puede desactivar."
    return True, ""


# ───────────────────────── el «mensaje del sistema» de cada solicitud ─────────────────────────
def _clp(n):
    try:
        return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")
    except (TypeError, ValueError):
        return "$0"


def resumen_por_mes(filas):
    """Para la hoja «Por mes e intención» del Excel: de [{fecha, intencion, centro, nos, val}] (una por OT que no se cobra) a
    [{mes 'AAAA-MM', intencion, centro, n, nos, val, sin_costo}] — cuántas y cuánto nos costó por mes, clasificación y centro de costo que lo asume.
    `nos` None = el técnico aún no declara su costo (no se inventa un $0: se cuenta aparte en `sin_costo`). Sin clasificar se dice."""
    acc = {}
    for f in (filas or []):
        fp = f.get("fecha")
        try:
            mes = fp.strftime("%Y-%m") if hasattr(fp, "strftime") else str(fp or "")[:7]
        except Exception:
            mes = ""
        if len(mes) != 7 or mes[4] != "-":
            mes = "Sin fecha"
        k = (mes, f.get("intencion") or "Sin clasificar", f.get("centro") or "(sin centro de costo)")
        a = acc.setdefault(k, {"n": 0, "nos": 0.0, "val": 0.0, "sin_costo": 0})
        a["n"] += 1
        a["nos"] += float(f.get("nos") or 0)
        a["val"] += float(f.get("val") or 0)
        a["sin_costo"] += 1 if f.get("nos") is None else 0
    out = [dict(mes=k[0], intencion=k[1], centro=k[2], **v) for k, v in acc.items()]
    out.sort(key=lambda x: (x["mes"], -x["nos"], x["intencion"], x["centro"]))
    return out


def observaciones_txt(obs):
    """Los textos de las observaciones de un documento de Random, venga como lista de documentos con su lista `observaciones` [{texto}], como
    lista de {texto} o como lista de textos. Sin repetidos y en orden."""
    out = []

    def _agrega(x):
        if isinstance(x, dict):
            if x.get("texto") and str(x["texto"]).strip() not in out:
                out.append(str(x["texto"]).strip())
            for y in (x.get("observaciones") or []):
                _agrega(y)
        elif isinstance(x, str) and x.strip() and x.strip() not in out:
            out.append(x.strip())
    for o in (obs or []):
        _agrega(o)
    return out


def mensaje_sistema(tipo, ctx=None):
    """Lo que el SISTEMA le dice a quien autoriza (además del argumento de quien pide): qué pasó, con las cifras. Se arma en el servidor con
    datos del servidor (nunca con lo que mande el navegador). Máx. 600 caracteres."""
    c = ctx or {}
    ot = f" en {c['numero_ot']}" if c.get("numero_ot") else ""
    if tipo == "cerrar_con_perdida":
        txt = (f"La OT{ot} cierra con una pérdida de {_clp(c.get('perdida'))}: se cobró {_clp(c.get('cobre'))} y el proveedor cobró "
               f"{_clp(c.get('me_cobraron'))}.")
    elif tipo == "exceder_saldo":
        sp = c.get("saldo_pedido") or {}
        partes = [f"{k} {_clp(v)}" for k, v in sp.items() if v]
        txt = ("Se quiere cobrar por encima del saldo de la línea del documento" + (": " + ", ".join(partes) if partes else "")
               + f"{ot}. El cobro no puede pasar del saldo sin esta autorización.")
    elif tipo == "cerrar_sin_documento":
        txt = f"La OT{ot} no tiene una factura ni boleta de Random para cerrarla: el cobro no queda respaldado por un documento."
    elif tipo == "crear_sin_documento":
        txt = "La OT se quiere crear sin un documento de Random (factura, boleta, nota de venta o cotización) que respalde el cobro."
    elif tipo == "documento_observado":
        obs = [str(o).strip() for o in (c.get("observaciones") or []) if str(o).strip()]
        txt = "El documento de Random queda observado" + (": " + " ".join(obs) if obs else " (más de 3 meses o productos sin saldo).")
    elif tipo == "cobro_cero":
        fam = FAMILIAS_TXT.get((c.get("familia") or "").strip().lower(), "")
        txt = f"La OT{ot} se declara en $0" + (f" como {fam}" if fam else "") + ": no se le cobra al cliente."
    else:
        txt = ""
    return txt[:600]

# -*- coding: utf-8 -*-
"""Movimientos (OT) de CheckWMS asociados a un documento: quién, cuándo, en qué estado.

Funciones PURAS (sin red ni base de datos) para poder probarlas. Check es SOLO LECTURA (REGLA #4.4):
este módulo nunca habla con Check, solo ordena filas que ya trajo `_checkwms_get` (app.py).

Daniel (2026-10-02): "necesito que me identifiques dinámicamente quién piqueó el producto, el responsable,
cuándo, hora, si lo asignó a alguien… toda la información que pueda sacar de Check, con usuario, fecha y todo".

El Swagger de Check declara la respuesta de GetStockTrazabilidad como genérica (`response: nullable`), así que
acá NO se supone ningún nombre de campo salvo los que ILUS ya usa desde agosto (ot, tipoOT, estado, doc,
entidad, feInicioOT, fechaFin, ua, codigo, descripcion, sol, ejec). Lo demás se reconoce por su NOMBRE
(usuario/operador/picker/asignado… para personas; fecha/inicio/fin/hora… para momentos) y todo campo con valor
se muestra tal cual en `datos`, para que no se pierda nada de lo que Check entregue."""
import re
from datetime import datetime
from functools import lru_cache

try:                                   # Chile: zoneinfo maneja el cambio de hora solo (REGLA #6)
    from zoneinfo import ZoneInfo
    _CHILE = ZoneInfo("America/Santiago")
except Exception:                      # pragma: no cover - Python sin zoneinfo/tzdata
    _CHILE = None

# Nombres de campo ya conocidos (clave normalizada → etiqueta para la persona)
ETIQUETAS = {
    "ot": "N° de OT", "codot": "N° de OT", "tipoot": "Tipo de OT", "estado": "Estado", "estot": "Estado de la OT",
    "feinicioot": "Inicio", "fechafin": "Fin", "fefinot": "Fin", "doc": "Documento", "entidad": "Cliente (según Check)",
    "ua": "UA (unidad de armado)", "codigo": "SKU", "descripcion": "Descripción", "sol": "Cantidad solicitada",
    "ejec": "Cantidad ejecutada", "bodega": "Bodega", "ubicacion": "Ubicación", "ubiorigen": "Ubicación de origen",
    "ubidest": "Ubicación de destino", "lote": "Lote", "familia": "Familia", "motivo": "Motivo",
}
# Campos que nombran a una PERSONA (usuario, operador, picker, responsable, asignado a…)
_PERSONA = re.compile(r"(usuario|usu|user|operador|operario|oper|picker|pickeador|responsable|resp|asignado|asig|ejecutor|revisor|login|(?<=[a-z])por$)")
# …salvo que el nombre del campo diga que es otra cosa (un id, una cantidad, una ubicación, una fecha, un estado)
_NO_PERSONA = re.compile(r"(uid|guid|cant|stock|ubi|codigo|descripcion|fecha|^fe|hora|estado|sol$|ejec$)")
_MOMENTO = re.compile(r"(fecha|fec|^fe|hora|^fh|inicio|fin$|date|time|creac|cread|created|updat|modific|actualiz|asign|pick|revis|despach|termin|cierre|ingreso|salida)")
# Un código numérico (ej. usuario = 1534) solo cuenta como persona si el campo lo dice con todas sus letras
_PERSONA_FUERTE = re.compile(r"(usuario|user|operador|operario|picker|pickeador|responsable)")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
_DOC_RE = re.compile(r"^\s*([A-Za-z]{2,5})[\s\-_./:]*0*([0-9]+)\s*$")
_CLAVES_DOC = ("doc", "documento", "docerp", "docref", "docreferencia", "nrodoc", "numdoc", "numerodocumento")
_CLAVES_OT = ("ot", "codot", "numeroot", "nroot")


@lru_cache(maxsize=1024)
def _clave(k):
    """Nombre de campo normalizado: minúsculas y solo letras/dígitos (feInicioOT → feinicioot). Con memoria: el volcado
    de Check tiene ~10 mil filas con los mismos pocos nombres de campo."""
    return re.sub(r"[^a-z0-9]", "", str(k or "").lower())


# Palabras que los nombres de campo de Check traen sin tilde (asignacion, ubicacion…): se muestran bien escritas
_TILDES = {"asignacion": "asignación", "ubicacion": "ubicación", "descripcion": "descripción", "revision": "revisión",
           "operacion": "operación", "creacion": "creación", "modificacion": "modificación", "transaccion": "transacción",
           "preparacion": "preparación", "despacho": "despacho", "autorizacion": "autorización", "recepcion": "recepción",
           "ejecucion": "ejecución", "confirmacion": "confirmación", "observacion": "observación", "cantidad": "cantidad"}


def etiqueta(campo):
    """Etiqueta legible de un campo: la conocida, o el nombre separado en palabras (usuarioPicking → Usuario picking)."""
    k = _clave(campo)
    if k in ETIQUETAS:
        return ETIQUETAS[k]
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(campo or "").strip())
    s = re.sub(r"[_\-]+", " ", s).strip()
    s = " ".join(_TILDES.get(w.lower(), w) for w in s.split())
    return (s[:1].upper() + s[1:].lower()) if s else ""


def momento(valor):
    """datetime del valor si parece una fecha/hora (ISO o dd/mm/aaaa [hh:mm]); si no, None."""
    if valor in (None, "") or isinstance(valor, bool) or isinstance(valor, (int, float)):
        return None
    s = str(valor).strip()
    if len(s) < 8 or not re.search(r"\d", s) or not re.search(r"[-/:]", s):   # '20261002' suelto no es una fecha
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def formatear_momento(valor):
    """«dd/mm/aaaa hh:mm». Si trae zona horaria se pasa a hora de Chile; si no, se muestra tal como la entrega Check."""
    d = momento(valor)
    if d is None:
        return None
    if d.tzinfo is not None and _CHILE is not None:
        d = d.astimezone(_CHILE)
    sin_hora = (d.hour, d.minute, d.second) == (0, 0, 0) and not re.search(r"\d:\d", str(valor))
    return d.strftime("%d/%m/%Y" if sin_hora else "%d/%m/%Y %H:%M")


def doc_clave(valor):
    """('FCV', '11155') para 'FCV-0000011155' / 'FCV 11155' / 'fcv11155'; None si no parece un documento."""
    m = _DOC_RE.match(str(valor or ""))
    return (m.group(1).upper(), m.group(2)) if m else None


def _objetivo(tipo, num):
    digitos = re.sub(r"\D", "", str(num or ""))
    if not tipo or not digitos:
        return None
    return (str(tipo).strip().upper()[:5], digitos.lstrip("0") or "0")


def filas_del_documento(filas, tipo, num):
    """Filas de movimientos que corresponden al documento ERP (tipo, número). Reconoce 'FCV-0000011155' en el
    campo doc, o tipo y número en campos separados; los ceros a la izquierda no importan."""
    obj = _objetivo(tipo, num)
    if not obj or not isinstance(filas, list):
        return []
    out = []
    for f in filas:
        if not isinstance(f, dict):
            continue
        candidatos, tipo_f, num_f = [], None, None
        for k, v in f.items():
            kn = _clave(k)
            if kn in _CLAVES_DOC and v not in (None, ""):
                candidatos.append(v)
            if kn in ("tipodoc", "tipodocumento", "tido"):
                tipo_f = v
            if kn in ("numdoc", "numerodocumento", "nudo", "nrodoc"):
                num_f = v
        if tipo_f and num_f:
            candidatos.append(f"{tipo_f}-{num_f}")
        if any(doc_clave(c) == obj for c in candidatos):
            out.append(f)
    return out


def catalogo_campos(filas):
    """Todos los nombres de campo que aparecen en las filas (para saber QUÉ entrega Check)."""
    return sorted({k for f in (filas or []) if isinstance(f, dict) for k in f.keys()})


def _texto(v):
    return "" if v is None else str(v).strip()


def _es_persona(campo, valor):
    s = _texto(valor)
    if not s or isinstance(valor, bool):
        return False
    if _UUID.match(s) or momento(s) is not None:
        return False
    k = _clave(campo)
    if _NO_PERSONA.search(k):
        return False
    if s.replace(".", "").replace(",", "").isdigit():          # un número: solo si el campo dice «usuario/operador/…»
        return bool(_PERSONA_FUERTE.search(k))
    return bool(_PERSONA.search(k))


def _es_momento(campo, valor):
    return bool(_MOMENTO.search(_clave(campo))) and momento(valor) is not None


def _num(v):
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _valor_ot(fila):
    for k, v in fila.items():
        if _clave(k) in _CLAVES_OT and v not in (None, ""):
            return _texto(v)
    return ""


def resumir_ot(filas):
    """Resumen legible de UNA OT (todas sus filas): estado, inicio/fin, personas, momentos, líneas y todos los datos."""
    filas = [f for f in (filas or []) if isinstance(f, dict)]
    if not filas:
        return None
    base = filas[0]
    por_clave = {_clave(k): v for k, v in base.items()}

    def pick(*claves):
        for c in claves:
            if por_clave.get(c) not in (None, ""):
                return por_clave[c]
        return None

    personas, momentos, vistos_p, vistos_m = [], [], set(), set()
    for f in filas:
        for k, v in f.items():
            if _texto(v) == "":
                continue
            if _es_persona(k, v):
                par = (_clave(k), _texto(v))
                if par not in vistos_p:
                    vistos_p.add(par)
                    personas.append({"campo": k, "etiqueta": etiqueta(k), "valor": _texto(v)})
            elif _es_momento(k, v):
                par = (_clave(k), _texto(v))
                if par not in vistos_m:
                    vistos_m.add(par)
                    momentos.append({"campo": k, "etiqueta": etiqueta(k), "valor": formatear_momento(v), "orden": momento(v)})
    momentos.sort(key=lambda m: (m["orden"].replace(tzinfo=None) if m["orden"] else datetime.max))
    for m in momentos:
        m.pop("orden", None)

    lineas, unidades = [], 0.0
    for f in filas:
        p = {_clave(k): v for k, v in f.items()}
        ejec, sol = _num(p.get("ejec")), _num(p.get("sol"))
        unidades += ejec if ejec is not None else 0
        lineas.append({
            "sku": _texto(p.get("codigo")), "descripcion": _texto(p.get("descripcion")), "ua": _texto(p.get("ua")),
            "solicitado": sol, "ejecutado": ejec,
            "origen": _texto(p.get("ubiorigen") or p.get("ubicacionorigen")), "destino": _texto(p.get("ubidest") or p.get("ubicaciondestino")),
        })

    datos = []
    for k, v in base.items():
        if _texto(v) == "":
            continue
        fmt = formatear_momento(v) if _es_momento(k, v) else None
        datos.append({"campo": k, "etiqueta": etiqueta(k), "valor": fmt or _texto(v)})

    return {
        "ot": _valor_ot(base), "tipo": _texto(pick("tipoot")), "estado": _texto(pick("estado", "estot")),
        "inicio": formatear_momento(pick("feinicioot")), "fin": formatear_momento(pick("fechafin", "fefinot")),
        "personas": personas, "momentos": momentos, "lineas": lineas, "n_lineas": len(lineas), "unidades": unidades,
        "datos": datos, "n_datos": len(datos),
    }


def agrupar_por_ot(filas):
    """Filas del documento → lista de OT resumidas, la más reciente primero (por inicio, si se puede)."""
    grupos, orden = {}, []
    for f in filas or []:
        if not isinstance(f, dict):
            continue
        clave = _valor_ot(f) or f"sin-ot-{len(orden)}"
        if clave not in grupos:
            grupos[clave] = []
            orden.append(clave)
        grupos[clave].append(f)
    ots = [resumir_ot(grupos[c]) for c in orden]
    ots = [o for o in ots if o]

    def _clave_orden(o):
        d = momento(next((m["valor"] for m in o["momentos"] if m["valor"]), None) or "")
        return d.replace(tzinfo=None) if d else datetime.min
    ots.sort(key=_clave_orden, reverse=True)
    return ots


def resumen_para_log(filas_doc, filas_todas):
    """Texto corto con los NOMBRES de campo (y valores solo de personas, momentos, OT y estado) para dejar en el
    log de la aplicación qué entrega Check sin volcar datos de clientes."""
    campos = catalogo_campos(filas_doc or filas_todas)
    ejemplo = {}
    for f in (filas_doc or filas_todas or [])[:1]:
        for k, v in f.items():
            if _texto(v) and (_es_persona(k, v) or _es_momento(k, v) or _clave(k) in ("ot", "estado", "tipoot", "estot")):
                ejemplo[k] = _texto(v)[:40]
    # Cómo escribe Check el campo «doc» (p. ej. 'FCV-0000011155' o 'FCV 11155'): sin ese dato no se sabe si un documento «no tiene OT» o
    # si simplemente no se está reconociendo su formato (2026-10-02: el retiro real salió con filas_doc=0 y 10.203 movimientos).
    docs_ej = []
    for f in (filas_todas or [])[:6000]:
        if isinstance(f, dict):
            v = _texto(f.get("doc"))[:20]
            if v and v not in docs_ej:
                docs_ej.append(v)
                if len(docs_ej) >= 6:
                    break
    return f"filas_doc={len(filas_doc or [])} filas_total={len(filas_todas or [])} campos={campos} ejemplo={ejemplo} doc_ejemplos={docs_ej}"

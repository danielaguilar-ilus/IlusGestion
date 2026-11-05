# -*- coding: utf-8 -*-
"""🚚 FLOTA · LECTOR DE LICENCIAS DE CONDUCIR DE CHILE — parte PURA (sin Flask, sin base, sin motor de OCR).

Daniel, 05-nov-2026: «debemos tener restricciones para las malas operaciones que logística está generando: un scanner de licencias, que
se pueda hacer por el celular o subiendo una foto, y que mediante ese scanner se agregue la licencia y sobre todo la fecha, así evitamos
que Milagros cree choferes con su usuario. Fíjate que en ese espacio dice "fecha de control 29/10/2029"». Eligió el OCR PROPIO (Tesseract en
el servidor): la foto de la licencia, con su RUT y su nombre, no sale de ILUS.

Qué trae una licencia chilena (y de dónde sale cada dato):
  · «CLASE»                     → clase(s): A1…A5, B, C, D, E, F.
  · «Nº DE LICENCIA»            → el RUT de la persona (con su dígito verificador).
  · «NOMBRES» y «APELLIDOS»     → el nombre completo.
  · «FECHA ÚLTIMO CONTROL»      → cuándo se controló por última vez.
  · «FECHA DE CONTROL»          → cuándo vence: el PRÓXIMO control. Es la fecha que importa.

Este módulo recibe el TEXTO que devolvió el OCR (con sus errores de lectura) y lo convierte en datos. Nunca da por buena una lectura dudosa:
un RUT cuyo dígito verificador no cuadra no se acepta, una fecha imposible no se acepta, y lo que no se pudo leer queda vacío y se avisa.
`evaluar` compara la lectura con lo que se está registrando y dice qué NO coincide (RUT, usuario de ILUS, clase, vencimiento).

Correr las pruebas:  python -m pytest tests/test_flota_licencia_lector.py -q -p no:warnings
"""
import re
import unicodedata
from collections import Counter
from datetime import date

import flota_reglas as fr

CLASES = ("A1", "A2", "A3", "A4", "A5", "B", "C", "D", "E", "F")
_PARTICULAS = {"DE", "DEL", "LA", "LAS", "LOS", "Y", "E", "SAN", "SANTA"}

# Avisos que acompañan a una lectura (códigos estables; el texto para la persona sale de AVISOS_TEXTO).
AVISOS_TEXTO = {
    "sin_rut": "No pude leer el RUT (Nº de licencia).",
    "rut_invalido": "Leí un RUT, pero su dígito verificador no cuadra: puede estar mal leído.",
    "varios_rut": "Encontré más de un RUT distinto en la foto.",
    "sin_fecha": "No pude leer la fecha de control (el vencimiento).",
    "una_sola_fecha": "Solo pude leer una fecha y no sé si es la de control (vencimiento) o la del último control: mírala en la foto y elígela.",
    "fecha_sin_etiqueta": "No pude comprobar cuál es la «fecha de control»: tomé la más lejana.",
    "vence_antes_del_ultimo": "La fecha de control es anterior al último control: revísala.",
    "sin_clase": "No pude leer la clase de licencia.",
    "clase_inferida": "No vi la palabra «Clase»: la clase puede estar mal leída.",
    "sin_nombre": "No pude leer el nombre.",
    "fecha_poco_segura": "Solo una lectura vio la fecha de control: compárala con la foto antes de guardar.",
}


# ── Texto ──────────────────────────────────────────────────────────────────
def _sin_acentos(t):
    return "".join(c for c in unicodedata.normalize("NFD", str(t or "")) if unicodedata.category(c) != "Mn")


def normalizar_texto(texto):
    """Mayúsculas, sin acentos, sin símbolos raros y con una línea por renglón del OCR."""
    t = _sin_acentos(texto).upper().replace("\r", "\n")
    t = t.replace("º", "O").replace("°", "O").replace("Nº", "N")
    t = re.sub(r"[^\w\s/\-.,:;]", " ", t, flags=re.U)
    lineas = [re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n")]
    return "\n".join(ln for ln in lineas if ln)


def _digitos(t):
    """En un pedazo que debería ser número, las letras que el OCR confunde con dígitos vuelven a ser dígitos."""
    return t.translate(str.maketrans({"O": "0", "Q": "0", "D": "0", "I": "1", "L": "1", "|": "1", "Z": "2", "S": "5", "B": "8", "G": "6"}))


# ── Fechas ─────────────────────────────────────────────────────────────────
_FECHA = re.compile(r"(?<![0-9A-Z])([0-3OIL]?[0-9OIL])\s*[/\-.,]\s*([01OIL]?[0-9OIL])\s*[/\-.,]\s*([2Z][0-9OIL]{3})(?![0-9])")


def _fecha_de(d, m, a):
    try:
        f = date(int(_digitos(a)), int(_digitos(m)), int(_digitos(d)))
    except ValueError:
        return None
    return f if date(2000, 1, 1) <= f <= date(2100, 12, 31) else None


def _fechas(texto):
    """[(fecha, contexto, linea)] de las fechas dd/mm/aaaa del texto, en el orden en que aparecen. El contexto es lo que viene escrito
    antes en la misma línea (y la anterior si la etiqueta quedó arriba)."""
    out = []
    lineas = texto.split("\n")
    for i, ln in enumerate(lineas):
        for m in _FECHA.finditer(ln):
            f = _fecha_de(m.group(1), m.group(2), m.group(3))
            if f is None:
                continue
            ctx = ln[:m.start()]
            if len(ctx.strip()) < 6 and i > 0:
                ctx = lineas[i - 1] + " " + ctx
            out.append((f, ctx, i))
    return out


def _es_ultimo(ctx):
    return "ULTIM" in ctx or "ULT." in ctx or "ULT " in ctx


def _es_control(ctx):
    return "CONTROL" in ctx or "VENC" in ctx or "VALID" in ctx


# ── RUT ────────────────────────────────────────────────────────────────────
_RUT = re.compile(r"(?<![0-9A-Z])([0-9OIL]{1,2}[.,\s]?[0-9OIL]{3}[.,\s]?[0-9OIL]{3})\s*[-–—_.]?\s*([0-9KSBOIZ])(?![0-9A-Z])")


def _rut_de(cuerpo, dv):
    """'25.547.065','5' → '25547065-5' si existe de verdad; si no, None."""
    c = re.sub(r"\D", "", _digitos(cuerpo))
    d = dv.upper()
    d = d if d == "K" else _digitos(d)
    n = fr.normalizar_rut(c + d)
    return n if n and fr.rut_valido(n) else None


def _ruts_leidos(texto):
    """Todo lo que parece un RUT en el texto, sea o no un RUT que exista: ['25647065-5', …] (solo dígitos y verificador). Sirve para
    comprobar el RUT que declara la persona aunque el OCR haya leído mal una cifra."""
    out = []
    for ln in texto.split("\n"):
        for m in _RUT.finditer(ln):
            c = re.sub(r"\D", "", _digitos(m.group(1)))
            d = m.group(2).upper()
            d = d if d == "K" else _digitos(d)
            if len(c) in (7, 8) and re.fullmatch(r"[0-9K]", d):
                out.append(c + "-" + d)
    return out


# Cifras que el OCR confunde entre sí en esta tipografía (se vio un 5 leído como 6 en una licencia real).
_CONFUNDIBLES = {frozenset(p) for p in ("56", "08", "38", "17", "14", "68", "89", "06", "35", "27", "49", "03")}


def rut_parecido(leido, esperado):
    """¿`leido` es el mismo RUT que `esperado` con a lo más UNA cifra confundida por el OCR (5↔6, 0↔8, 1↔7…)? Los dos 'cuerpo-dv'."""
    a, b = re.sub(r"[^0-9K]", "", str(leido or "").upper()), re.sub(r"[^0-9K]", "", str(esperado or "").upper())
    if not a or len(a) != len(b):
        return False
    if a == b:
        return True
    dif = [(x, y) for x, y in zip(a, b) if x != y]
    return len(dif) == 1 and frozenset(dif[0]) in _CONFUNDIBLES


def rut_coincide(lectura, esperado):
    """El RUT esperado está en la foto: lo leído (con dígito verificador válido) o cualquier RUT mal leído que se le parezca."""
    esp = fr.normalizar_rut(esperado)
    if not esp:
        return False
    l = lectura or {}
    if l.get("rut") == esp:
        return True
    return any(rut_parecido(r, esp) for r in (l.get("ruts_leidos") or []))


def _ruts(texto):
    """([RUT válidos en el orden en que aparecen], hubo_alguno_invalido)."""
    validos, invalido = [], False
    for ln in texto.split("\n"):
        for m in _RUT.finditer(ln):
            r = _rut_de(m.group(1), m.group(2))
            if r:
                validos.append(r)
            else:
                invalido = True
    return validos, invalido


# ── Clase ──────────────────────────────────────────────────────────────────
_TOKEN_CLASE = {"AI": "A1", "AL": "A1", "A1": "A1", "A2": "A2", "A3": "A3", "A4": "A4", "A5": "A5", "B": "B", "C": "C", "D": "D", "E": "E", "F": "F"}


def _clases_tras_etiqueta(texto):
    for ln in texto.split("\n"):
        m = re.search(r"\bCLASES?\b\s*[:\-]?\s*(.{0,30})", ln)
        if not m:
            continue
        found = []
        for tok in re.split(r"[\s,;/\-Y.]+", m.group(1)):
            c = _TOKEN_CLASE.get(tok.strip())
            if c is None:
                break
            if c not in found:
                found.append(c)
        if found:
            return found
    return []


def _orden_clases(cs):
    return [c for c in CLASES if c in set(cs)]


# ── Nombre ─────────────────────────────────────────────────────────────────
_ETIQUETAS = ("CLASE", "N DE LICENCIA", "NO DE LICENCIA", "NUMERO", "MUNICIPALIDAD", "NOMBRES", "NOMBRE", "APELLIDOS", "DIRECCION",
              "FECHA", "LICENCIA", "REPUBLICA", "CONDUCTOR")


def _es_etiqueta(ln):
    return any(ln.startswith(e) for e in _ETIQUETAS)


def _valor_de(texto, etiqueta):
    """Lo que está escrito a continuación de «NOMBRES» / «APELLIDOS»: en la misma línea o, si quedó solo, en la siguiente."""
    lineas = texto.split("\n")
    for i, ln in enumerate(lineas):
        m = re.match(r"^%s\b\s*[:\-]?\s*(.*)$" % etiqueta, ln)
        if not m:
            continue
        resto = re.sub(r"[^A-ZÑ ]", "", m.group(1)).strip()
        if len(resto) >= 3:
            return re.sub(r"\s+", " ", resto)
        for sig in lineas[i + 1:i + 3]:
            if _es_etiqueta(sig):
                break
            v = re.sub(r"[^A-ZÑ ]", "", sig).strip()
            if len(v) >= 3:
                return re.sub(r"\s+", " ", v)
    return None


def palabras_del_nombre(nombre):
    """Las palabras que identifican a una persona (sin acentos, sin 'de', 'del'…): para comparar nombres sin importar el orden."""
    pal = re.findall(r"[A-ZÑ]{3,}", _sin_acentos(nombre).upper())
    return {p for p in pal if p not in _PARTICULAS}


def palabras_del_texto(texto_normalizado):
    """Todas las palabras (de 3 letras o más, sin 'de', 'del'…) que el OCR leyó en la licencia, ordenadas. Sirven para comprobar que el nombre de
    una persona está en la licencia aunque las etiquetas «Nombres» y «Apellidos» no se hayan podido leer (están en cursiva, bajo un sello)."""
    return sorted({p for p in re.findall(r"[A-ZÑ]{3,}", texto_normalizado or "") if p not in _PARTICULAS})


def nombre_en_palabras(palabras, nombre, minimo=2):
    """¿Están en la licencia al menos `minimo` palabras del nombre de esta persona?"""
    return len(palabras_del_nombre(nombre) & set(palabras or ())) >= minimo


def _nombre_coincide(lectura, nombre):
    """El nombre leído en la licencia (o las palabras sueltas que se leyeron) coincide con el de esta persona."""
    l = lectura or {}
    return bool((l.get("nombre") and nombres_parecidos(l["nombre"], nombre)) or nombre_en_palabras(l.get("palabras"), nombre))


def nombres_parecidos(a, b, minimo=2):
    """¿Son la misma persona? Comparten al menos `minimo` palabras del nombre. Con una sola palabra en común no alcanza (un nombre de una
    sola palabra nunca se da por comprobado: ahí se pide el RUT del perfil)."""
    pa, pb = palabras_del_nombre(a), palabras_del_nombre(b)
    if not pa or not pb:
        return False
    return len(pa & pb) >= minimo


# ── La lectura ─────────────────────────────────────────────────────────────
def interpretar_texto(texto_ocr):
    """Convierte el texto del OCR en una lectura. Siempre devuelve el mismo molde:
    {rut, rut_formato, vence, ultimo_control, clases, nombres, apellidos, nombre, avisos[], legible}
    `legible` = se pudo leer un RUT que existe Y la fecha de control: lo mínimo para dar por buena la licencia."""
    t = normalizar_texto(texto_ocr)
    avisos = []

    # RUT
    ruts, invalido = _ruts(t)
    rut = None
    if ruts:
        cuenta = Counter(ruts)
        rut = max(ruts, key=lambda r: (cuenta[r], -ruts.index(r)))
        if len(cuenta) > 1:
            avisos.append("varios_rut")
    else:
        avisos.append("rut_invalido" if invalido else "sin_rut")

    # Fechas
    fechas = _fechas(t)
    vistas = sorted({f[0] for f in fechas})
    vence = ultimo = None
    if fechas:
        no_ultimas = [f for f in fechas if not _es_ultimo(f[1])]
        con_control = [f for f in no_ultimas if _es_control(f[1])]
        ultimas = [f for f in fechas if _es_ultimo(f[1])]
        if ultimas:
            ultimo = max(f[0] for f in ultimas)
        if con_control:
            vence = max(f[0] for f in con_control)
        elif no_ultimas and len(vistas) >= 2:
            vence = max(f[0] for f in no_ultimas)
            avisos.append("fecha_sin_etiqueta")
        elif no_ultimas:
            avisos.append("una_sola_fecha")                  # una fecha sola y sin etiqueta: podía ser la del último control, NO se adivina
        elif len(ultimas) >= 2:
            vence = max(f[0] for f in ultimas)       # las dos dicen «último»: la más lejana es la que vale
            ultimo = min(f[0] for f in ultimas)
            avisos.append("fecha_sin_etiqueta")
        if vence is None and "una_sola_fecha" not in avisos:
            avisos.append("sin_fecha")
        if vence and ultimo and vence < ultimo:
            avisos.append("vence_antes_del_ultimo")
    else:
        avisos.append("sin_fecha")

    # Clase
    clases = _orden_clases(_clases_tras_etiqueta(t))
    if not clases:
        avisos.append("sin_clase")

    # Nombre
    nombres, apellidos = _valor_de(t, "NOMBRES?"), _valor_de(t, "APELLIDOS?")
    nombre = " ".join(x for x in (nombres, apellidos) if x) or None
    if not nombre:
        avisos.append("sin_nombre")

    leidos = _ruts_leidos(t)
    return {"rut": rut, "rut_formato": fr.rut_formato(rut) if rut else None, "vence": vence, "ultimo_control": ultimo, "clases": clases,
            "nombres": nombres, "apellidos": apellidos, "nombre": nombre, "avisos": avisos, "palabras": palabras_del_texto(t),
            "fechas": vistas, "ruts_leidos": leidos, "legible": bool((rut or leidos) and (vence or vistas))}


def puntaje(lectura):
    """Qué tan completa es una lectura (para elegir la mejor entre varias pasadas del OCR)."""
    return (3 if lectura.get("rut") else 0) + (3 if lectura.get("vence") else 0) + (1 if lectura.get("clases") else 0) + \
        (1 if lectura.get("nombre") else 0) - (1 if "fecha_sin_etiqueta" in (lectura.get("avisos") or []) else 0)


def fusionar(lecturas):
    """Une varias lecturas de la MISMA foto (el OCR se corre con varios filtros): en cada dato gana el valor que más veces se leyó, y a
    igualdad el de la lectura más completa. Así un filtro que lee mal una cifra pierde frente a los que la leyeron bien."""
    lecturas = [l for l in (lecturas or []) if l]
    if not lecturas:
        return interpretar_texto("")
    orden = sorted(lecturas, key=lambda l: -puntaje(l))

    def elegir(campo):
        vals = [l[campo] for l in orden if l.get(campo)]
        if not vals:
            return None
        cuenta = Counter(map(str, vals))
        mejor = max(cuenta.values())
        for v in vals:                      # a igualdad de votos, el de la lectura más completa
            if cuenta[str(v)] == mejor:
                return v
        return vals[0]

    rut, vence, ultimo = elegir("rut"), elegir("vence"), elegir("ultimo_control")
    clases = elegir("clases") or []
    nombres, apellidos = elegir("nombres"), elegir("apellidos")
    nombre = " ".join(x for x in (nombres, apellidos) if x) or None
    avisos = []
    if not rut:
        avisos.append("rut_invalido" if any("rut_invalido" in (l.get("avisos") or []) for l in lecturas) else "sin_rut")
    if len({l["rut"] for l in lecturas if l.get("rut")}) > 1:
        avisos.append("varios_rut")
    if not vence:
        avisos.append("sin_fecha")
    elif not any(l.get("vence") == vence and "fecha_sin_etiqueta" not in (l.get("avisos") or []) for l in lecturas):
        avisos.append("fecha_sin_etiqueta")
    if vence and sum(1 for l in lecturas if l.get("vence") == vence) < 2:
        avisos.append("fecha_poco_segura")                 # una sola pasada vio esta fecha: una cifra mal leída no tiene dígito verificador
    if vence and ultimo and vence < ultimo:
        avisos.append("vence_antes_del_ultimo")
    if not clases:
        avisos.append("sin_clase")
    if not nombre:
        avisos.append("sin_nombre")
    palabras = sorted({p for l in lecturas for p in (l.get("palabras") or ())})
    vistas = sorted({f for l in lecturas for f in (l.get("fechas") or ())})
    leidos = sorted({r for l in lecturas for r in (l.get("ruts_leidos") or ())})
    if vence is None and len(vistas) >= 2:
        # Cada pasada vio una fecha distinta y sola: entre todas hay dos, y la de control es la más lejana (el próximo control).
        vence = max(vistas)
        avisos = [a for a in avisos if a not in ("sin_fecha", "una_sola_fecha")] + ["fecha_sin_etiqueta"]
        if sum(1 for l in lecturas if vence in (l.get("fechas") or ())) < 2 and "fecha_poco_segura" not in avisos:
            avisos.append("fecha_poco_segura")
    elif vence is None and len(vistas) == 1:
        avisos = [a for a in avisos if a != "sin_fecha"]
        if "una_sola_fecha" not in avisos:
            avisos.append("una_sola_fecha")
    return {"rut": rut, "rut_formato": fr.rut_formato(rut) if rut else None, "vence": vence, "ultimo_control": ultimo, "clases": clases,
            "nombres": nombres, "apellidos": apellidos, "nombre": nombre, "avisos": avisos, "palabras": palabras, "fechas": vistas,
            "ruts_leidos": leidos, "legible": bool((rut or leidos) and (vence or vistas))}


def lectura_a_json(lectura):
    """La lectura lista para mandarla al navegador (fechas en ISO y en texto de persona; avisos con su explicación)."""
    l = lectura or {}
    vence, ultimo = l.get("vence"), l.get("ultimo_control")
    return {"rut": l.get("rut"), "rut_formato": l.get("rut_formato"),
            "vence": vence.isoformat() if vence else None, "vence_fmt": fr.fecha_corta(vence) if vence else None,
            "ultimo_control": ultimo.isoformat() if ultimo else None, "ultimo_control_fmt": fr.fecha_corta(ultimo) if ultimo else None,
            "clases": list(l.get("clases") or []), "nombres": l.get("nombres"), "apellidos": l.get("apellidos"), "nombre": l.get("nombre"),
            "palabras": list(l.get("palabras") or []), "ruts_leidos": list(l.get("ruts_leidos") or []),
            "fechas": [{"iso": f.isoformat(), "fmt": fr.fecha_corta(f)} for f in (l.get("fechas") or [])], "legible": bool(l.get("legible")),
            "avisos": [{"codigo": c, "texto": AVISOS_TEXTO.get(c, c)} for c in (l.get("avisos") or [])]}


def fecha_de_control(lectura, declarada=None):
    """(fecha | None, error | None): la fecha de control que vale. Si la persona declara una, TIENE que ser una de las fechas que se vieron en la
    foto (no se puede inventar ni dejar la de otra licencia); si no declara, vale la que el sistema reconoció como de control."""
    l = lectura or {}
    vistas = list(l.get("fechas") or [])
    if declarada:
        if declarada in vistas or declarada == l.get("vence"):
            return declarada, None
        leidas = ", ".join(fr.fecha_corta(f) for f in vistas) or "ninguna"
        return None, (f"La fecha de control que indicas ({fr.fecha_corta(declarada)}) no aparece en la licencia escaneada (se leyó: {leidas}). "
                      "Elige la fecha de control tal como está en la foto, o escanéala de nuevo más de cerca.")
    if l.get("vence"):
        return l["vence"], None
    return None, ("No pude saber cuál es la fecha de control (vencimiento) en la foto. Elige la fecha de control entre las que leí, "
                  "o escanéala de nuevo más de cerca.")


# ── Comparar lo leído con lo que se está registrando ───────────────────────
MSG_ILEGIBLE = ("No pude leer la licencia. Toma la foto de frente, con buena luz, sin reflejos y con la licencia completa dentro del "
                "cuadro; o sube un escaneo más nítido.")


def _rut_leido_texto(l):
    """El RUT de la licencia como se lo mostramos a la persona: el que existe, o el primero que se alcanzó a leer (con dudas)."""
    if l.get("rut"):
        return fr.rut_formato(l["rut"])
    leidos = l.get("ruts_leidos") or []
    return (fr.rut_formato(leidos[0]) + " (leído con dudas)") if leidos else "no se pudo leer"


def evaluar(lectura, rut_chofer=None, nombre_chofer=None, usuario=None, clases_declaradas=None, hoy=None, vence_declarado=None):
    """(errores{campo: texto}, avisos[texto]) de comparar la licencia LEÍDA con el registro del chofer. Lo que declara la persona tiene que
    APARECER en la foto (el OCR se equivoca en una cifra, pero nadie puede inventar un dato que la foto no tiene):
      · rut: el RUT de la licencia tiene que ser el del chofer (igual, o con una sola cifra confundida por el OCR).
      · usuario_id: el usuario de ILUS elegido tiene que ser esa misma persona (su RUT del perfil, o —si no lo tiene— su nombre).
      · licencia_vence: la fecha de control tiene que ser una de las leídas (`vence_declarado`) y no estar vencida.
      · licencia_clases: no se pueden marcar clases que la licencia no trae.
    `usuario` = {"nombre": …, "rut": …} del usuario de ILUS elegido (rut puede venir vacío)."""
    hoy = hoy or date.today()
    errores, avisos = {}, []
    l = lectura or {}
    if not l.get("legible"):
        errores["licencia_archivo"] = MSG_ILEGIBLE
        faltan = [AVISOS_TEXTO[c] for c in (l.get("avisos") or []) if c in ("sin_rut", "rut_invalido", "sin_fecha")]
        if faltan:
            errores["licencia_archivo"] += " (" + " ".join(faltan) + ")"
        return errores, avisos
    leido = _rut_leido_texto(l)
    if rut_chofer:
        if not rut_coincide(l, rut_chofer):
            errores["rut"] = (f"El RUT de la licencia ({leido}) no es el del chofer ({fr.rut_formato(rut_chofer)}). "
                              "La licencia tiene que ser de la misma persona que se registra.")
        elif l.get("rut") != fr.normalizar_rut(rut_chofer):
            avisos.append("El RUT de la licencia se leyó con una cifra dudosa, pero coincide con el del chofer: compáralo con la foto.")
    if usuario is not None:
        rut_u = fr.normalizar_rut((usuario or {}).get("rut")) if (usuario or {}).get("rut") else None
        nombre_u = (usuario or {}).get("nombre") or "ese usuario"
        if rut_u:
            if not rut_coincide(l, rut_u):
                errores["usuario_id"] = (f"{nombre_u} tiene otro RUT ({fr.rut_formato(rut_u)}) que la licencia ({leido}): "
                                         "la licencia no es de esa persona. Cada chofer se registra con SU propio usuario de ILUS.")
        elif not _nombre_coincide(l, nombre_u):
            errores["usuario_id"] = (f"{nombre_u} no tiene RUT en su perfil y su nombre no coincide con el de la licencia "
                                     f"({l.get('nombre') or 'no se leyó'}): no puedo comprobar que sea su licencia. Elige el usuario de la "
                                     "persona dueña de la licencia, o pídele que complete su RUT en Mi cuenta.")
        else:
            avisos.append(f"{nombre_u} no tiene RUT en su perfil: se comprobó por el nombre. Conviene que lo complete en Mi cuenta.")
    if nombre_chofer and (l.get("nombre") or l.get("palabras")) and not _nombre_coincide(l, nombre_chofer):
        avisos.append(f"El nombre escrito ({nombre_chofer}) no se parece al de la licencia ({l.get('nombre') or 'no se pudo leer el nombre'}). Revísalo.")
    fecha, err_fecha = fecha_de_control(l, vence_declarado)
    if err_fecha:
        errores["licencia_vence"] = err_fecha
    elif fecha < hoy:
        errores["licencia_vence"] = f"La licencia venció el {fr.fecha_corta(fecha)}: no se puede registrar con una licencia vencida. Hay que renovarla y escanearla de nuevo."
    else:
        dias = (fecha - hoy).days
        if dias <= fr.DIAS_ALERTA:
            avisos.append(f"La licencia vence en {dias} día{'s' if dias != 1 else ''} ({fr.fecha_corta(fecha)}).")
    if clases_declaradas is not None:
        leidas = set(l.get("clases") or [])
        extra = [c for c in fr.normalizar_clases(clases_declaradas) if leidas and c not in leidas]
        if extra:
            errores["licencia_clases"] = (f"La licencia dice Clase {', '.join(l['clases'])}: no se puede marcar {', '.join(extra)}.")
        elif not leidas:
            avisos.append("No pude leer la clase en la licencia: la clase marcada queda sin comprobar.")
    for c in (l.get("avisos") or []):
        if c in ("varios_rut", "fecha_sin_etiqueta", "una_sola_fecha", "vence_antes_del_ultimo", "fecha_poco_segura"):
            avisos.append(AVISOS_TEXTO[c])
    return errores, avisos

"""Datos personales del usuario (formulario Nuevo/Editar usuario): fecha de
nacimiento y dirección validada con Google.

Módulo PURO: sin Flask ni base de datos, para poder probarlo solo. El RUT se
valida con validar_rut() de app.py (módulo 11). La regla espejo del lado del
navegador vive en static/ilus_persona_fields.js: si cambias una, cambia la otra.
"""
import re
from datetime import date

EDAD_MINIMA = 15
EDAD_MAXIMA = 100

# Caja que contiene a Chile continental, Juan Fernández, Isla de Pascua y la
# Antártica chilena. No es un mapa: solo descarta (0,0), coordenadas
# intercambiadas o basura. Google ya se consulta restringido a Chile.
_LAT_MIN, _LAT_MAX = -90.0, -17.0
_LNG_MIN, _LNG_MAX = -110.0, -53.0
_PLACE_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{10,200}$")
_FECHA_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

MSG_DIRECCION_SIN_VALIDAR = "Elige la dirección de la lista de sugerencias de Google para validarla."


def hoy_chile():
    """Fecha de hoy en Chile (el servidor corre en UTC)."""
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Santiago")).date()
    except Exception:
        return date.today()


def edad_en_anios(nacimiento, hoy):
    """Años cumplidos a la fecha `hoy`."""
    return hoy.year - nacimiento.year - (
        (hoy.month, hoy.day) < (nacimiento.month, nacimiento.day))


def validar_fecha_nacimiento(raw, hoy=None):
    """(ok, valor). Campo opcional: vacío es válido y devuelve None.
    ok=True → valor es un `date` (o None). ok=False → valor es el mensaje."""
    txt = (raw or "").strip()
    if not txt:
        return True, None
    if not _FECHA_ISO_RE.match(txt):
        return False, "La fecha de nacimiento no es válida. Escribe día, mes y año."
    try:
        f = date(int(txt[0:4]), int(txt[5:7]), int(txt[8:10]))
    except ValueError:
        return False, "Esa fecha de nacimiento no existe. Revisa el día y el mes."
    hoy = hoy or hoy_chile()
    if f > hoy:
        return False, "La fecha de nacimiento no puede ser futura."
    edad = edad_en_anios(f, hoy)
    if edad < EDAD_MINIMA:
        return False, f"La persona tendría menos de {EDAD_MINIMA} años. Revisa el año."
    if edad > EDAD_MAXIMA:
        return False, f"La persona tendría más de {EDAD_MAXIMA} años. Revisa el año."
    return True, f


def normalizar_direccion(texto):
    """Texto de dirección sin espacios repetidos, máximo 300 caracteres."""
    return re.sub(r"\s+", " ", str(texto or "")).strip()[:300]


def _a_float(v):
    try:
        x = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return x if x == x and abs(x) != float("inf") else None


def validar_geo_chile(lat, lng, place_id):
    """(ok, (lat, lng, place_id)) o (False, mensaje). Exige las tres cosas:
    una dirección validada con Google siempre trae coordenadas y place_id."""
    la, lo = _a_float(lat), _a_float(lng)
    pid = str(place_id or "").strip()
    if la is None or lo is None or not pid:
        return False, MSG_DIRECCION_SIN_VALIDAR
    if not (_LAT_MIN <= la <= _LAT_MAX and _LNG_MIN <= lo <= _LNG_MAX):
        return False, "La ubicación de la dirección no está en Chile. Elígela otra vez de la lista."
    if not _PLACE_ID_RE.match(pid):
        return False, MSG_DIRECCION_SIN_VALIDAR
    return True, (round(la, 7), round(lo, 7), pid)


def resolver_direccion(direccion, lat, lng, place_id, direccion_actual=None):
    """Decide qué hacer con la dirección al guardar.

    Devuelve (ok, dato):
      ok=True  → dato = {"accion", "direccion", "lat", "lng", "place_id"} con
                 accion 'limpiar' (se borra todo), 'guardar' (dirección nueva
                 validada) o 'mantener' (mismo texto que ya estaba guardado y
                 sin datos nuevos de Google: no se toca la ubicación).
      ok=False → dato = mensaje de error.

    `direccion_actual` es el texto ya guardado del usuario (None al crear). Una
    dirección vieja, escrita a mano antes de existir la validación, se puede
    dejar tal cual; en cuanto se cambia el texto tiene que venir validada."""
    texto = normalizar_direccion(direccion)
    if not texto:
        return True, {"accion": "limpiar", "direccion": None,
                      "lat": None, "lng": None, "place_id": None}
    if any(str(x or "").strip() for x in (lat, lng, place_id)):
        ok, geo = validar_geo_chile(lat, lng, place_id)
        if not ok:
            return False, geo
        return True, {"accion": "guardar", "direccion": texto,
                      "lat": geo[0], "lng": geo[1], "place_id": geo[2]}
    if direccion_actual is not None and normalizar_direccion(direccion_actual) == texto:
        return True, {"accion": "mantener", "direccion": texto,
                      "lat": None, "lng": None, "place_id": None}
    return False, MSG_DIRECCION_SIN_VALIDAR

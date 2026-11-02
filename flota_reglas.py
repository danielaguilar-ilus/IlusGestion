"""Reglas PURAS de la flota de ILUS Fitness (vehículos propios y, en las fases siguientes, choferes y solicitudes de uso).

Sin Flask, sin base de datos y sin red: todo lo que DECIDE algo vive acá y se prueba en tests/test_flota_vehiculos.py.
flota_module.py solo lee, escribe y pinta.

Pedido de Daniel (02-nov-2026): «registrar los datos de los camiones, tenerlos habilitados y poder habilitar uno nuevo». La flota NO se
fija en el código: hoy son un furgón y un camión, pero cada vehículo nuevo se ingresa a mano desde la pantalla. El sistema sugiere (tipos,
combustibles, clase de licencia) y la persona decide (REGLA #32). Este módulo no maneja precios ni costos.
"""
import re
from datetime import date, datetime
from decimal import Decimal

# Clases de licencia de conducir de Chile (Ley de Tránsito N°18.290). Réplica local a propósito de LICENCIA_CLASES_CHILE de app.py: este
# módulo es puro y no puede importar app.py. Si cambia una, cambia la otra.
LICENCIA_CLASES = {
    "A1": "Taxis",
    "A2": "Taxis, ambulancias, transporte de personas (10 a 17 pasajeros)",
    "A3": "Taxis, transporte escolar, ambulancias, buses (sin límite de asientos)",
    "A4": "Camiones simples de carga, peso bruto sobre 3.500 kg",
    "A5": "Camiones simples o articulados de carga, peso bruto sobre 3.500 kg",
    "B": "Vehículos particulares (autos, camionetas, furgones hasta 3.500 kg)",
    "C": "Motocicletas y motonetas",
    "D": "Maquinaria automotriz (tractores, palas mecánicas)",
    "E": "Vehículos de tracción animal",
    "F": "Vehículos policiales, de bomberos o Fuerzas Armadas",
}
ORDEN_CLASES = tuple(LICENCIA_CLASES)

# Sugerencias para el front: la persona puede escribir cualquier otro valor.
TIPOS_SUGERIDOS = ("Furgón", "Camión", "Camioneta", "Auto")
COMBUSTIBLES_SUGERIDOS = ("Diésel", "Bencina", "Eléctrico", "Híbrido", "Gas")

# Documentos con vencimiento de cada vehículo: (clave, nombre). La columna de la base es «<clave>_vence».
DOCUMENTOS = (
    ("revision_tecnica", "Revisión técnica"),
    ("permiso_circulacion", "Permiso de circulación"),
    ("soap", "SOAP"),
    ("seguro", "Seguro"),
)
DIAS_ALERTA = 30      # mismo umbral de «por vencer» que ya usa Transporte (_tr_estado_vencimiento)

# Columnas editables de flota_vehiculos, en el orden en que se guardan.
COLUMNAS = (
    "patente", "tipo", "marca", "modelo", "anio", "color", "vin",
    "capacidad_kg", "capacidad_m3", "combustible", "estanque_l", "km_actual",
    "licencia_clases",
    "revision_tecnica_vence", "permiso_circulacion_vence", "soap_vence", "seguro_vence",
    "notas",
)

ETIQUETAS = {
    "patente": "Patente", "tipo": "Tipo", "marca": "Marca", "modelo": "Modelo", "anio": "Año", "color": "Color", "vin": "N° de chasis",
    "capacidad_kg": "Capacidad de carga (kg)", "capacidad_m3": "Volumen (m³)", "combustible": "Combustible",
    "estanque_l": "Estanque (litros)", "km_actual": "Kilómetros", "licencia_clases": "Licencia que exige",
    "revision_tecnica_vence": "Revisión técnica vence", "permiso_circulacion_vence": "Permiso de circulación vence",
    "soap_vence": "SOAP vence", "seguro_vence": "Seguro vence", "notas": "Observaciones",
}

MOTIVO_MIN = 5
MOTIVO_MAX = 300
_FECHA_MIN = date(2000, 1, 1)
_FECHA_MAX = date(2100, 12, 31)


# ── Patente ────────────────────────────────────────────────────────────────
def normalizar_patente(raw):
    """'ab-12 34' → 'AB1234'. Solo letras y números, en mayúsculas."""
    return re.sub(r"[^A-Z0-9]", "", str(raw or "").upper())


def patente_valida(patente):
    """Entre 5 y 7 letras o números, con al menos una letra y un número. No se exige el formato exacto (AB1234 / ABCD12) para no trabar un
    remolque o un vehículo especial: la persona decide."""
    p = patente or ""
    return bool(re.fullmatch(r"[A-Z0-9]{5,7}", p)) and any(c.isalpha() for c in p) and any(c.isdigit() for c in p)


def patente_con_guion(patente):
    """AB1234 → AB-1234 · ABCD12 → ABCD-12 (como se escribe en Chile). Otro formato queda tal cual."""
    p = patente or ""
    if re.fullmatch(r"[A-Z]{2}\d{4}", p):
        return p[:2] + "-" + p[2:]
    if re.fullmatch(r"[A-Z]{4}\d{2}", p):
        return p[:4] + "-" + p[4:]
    return p


# ── Licencia ───────────────────────────────────────────────────────────────
def normalizar_clases(raw):
    """'b, a4' / ['A4','B'] / 'B A4' → ['A4','B'] (sin repetidas, en el orden oficial). Ignora lo que no sea una clase real."""
    if raw is None:
        return []
    partes = re.split(r"[,/;\s]+", raw) if isinstance(raw, str) else [str(x) for x in raw]
    vistas = {p.strip().upper() for p in partes}
    return [c for c in ORDEN_CLASES if c in vistas]


# ── Números y fechas ───────────────────────────────────────────────────────
def _entero(valor, minimo, maximo, miles=True):
    """None si viene vacío. Acepta 12500 o 12.500 (punto de miles). Levanta ValueError con el motivo en español."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return None
    if isinstance(valor, bool):
        raise ValueError("Debe ser un número.")
    if isinstance(valor, (int, float)):
        if isinstance(valor, float) and not valor.is_integer():
            raise ValueError("Debe ser un número entero.")
        n = int(valor)
    else:
        t = str(valor).strip().replace(" ", "")
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", t):
            t = t.replace(".", "")
        if not re.fullmatch(r"\d+", t):
            raise ValueError("Debe ser un número entero.")
        n = int(t)
    if n < minimo or n > maximo:
        if miles:
            raise ValueError(f"Debe estar entre {minimo:,} y {maximo:,}.".replace(",", "."))
        raise ValueError(f"Debe estar entre {minimo} y {maximo}.")
    return n


def _decimal(valor, minimo, maximo):
    """None si viene vacío. Acepta coma o punto decimal. Redondea a 2 decimales."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return None
    if isinstance(valor, bool):
        raise ValueError("Debe ser un número.")
    try:
        n = float(str(valor).strip().replace(" ", "").replace(",", "."))
    except ValueError:
        raise ValueError("Debe ser un número.")
    if n != n or n in (float("inf"), float("-inf")):
        raise ValueError("Debe ser un número.")
    if n < minimo or n > maximo:
        raise ValueError(f"Debe estar entre {minimo} y {maximo}.")
    return round(n, 2)


def fecha_o_none(valor):
    """None si viene vacía. Acepta 'AAAA-MM-DD' (lo que manda un <input type=date>), una date o un datetime. ValueError si no es una fecha
    razonable (entre los años 2000 y 2100)."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return None
    try:
        if isinstance(valor, datetime):
            f = valor.date()
        elif isinstance(valor, date):
            f = valor
        else:
            f = date.fromisoformat(str(valor).strip()[:10])
    except ValueError:
        raise ValueError("Fecha no válida.")
    if f < _FECHA_MIN or f > _FECHA_MAX:
        raise ValueError("Fecha fuera de rango.")
    return f


def estado_vencimiento(fecha, hoy, dias_alerta=DIAS_ALERTA):
    """'vencido' · 'por_vencer' (dentro de los próximos `dias_alerta` días) · 'vigente' · None si no hay fecha."""
    if fecha is None:
        return None
    if isinstance(fecha, datetime):
        fecha = fecha.date()
    dias = (fecha - hoy).days
    if dias < 0:
        return "vencido"
    if dias <= dias_alerta:
        return "por_vencer"
    return "vigente"


def texto_dias(dias):
    """Días que faltan (o pasaron) dichos como los diría una persona."""
    if dias == 0:
        return "Vence hoy"
    if dias == 1:
        return "Vence mañana"
    if dias == -1:
        return "Venció ayer"
    if dias > 1:
        return f"Vence en {dias} días"
    return f"Venció hace {-dias} días"


def fecha_corta(f):
    """date → 'dd/mm/aaaa' (nunca ISO crudo: REGLA #6)."""
    return f.strftime("%d/%m/%Y") if f else ""


def resumen_documentos(fila, hoy, dias_alerta=DIAS_ALERTA):
    """Los 4 documentos de un vehículo con su estado. `fila` trae '<clave>_vence' como date, 'AAAA-MM-DD' o None."""
    out = []
    for clave, nombre in DOCUMENTOS:
        try:
            f = fecha_o_none((fila or {}).get(clave + "_vence"))
        except ValueError:
            f = None
        estado = estado_vencimiento(f, hoy, dias_alerta)
        dias = (f - hoy).days if f else None
        out.append({
            "clave": clave, "nombre": nombre,
            "vence": f.isoformat() if f else None, "vence_fmt": fecha_corta(f),
            "estado": estado or "sin_fecha", "dias": dias,
            "texto": texto_dias(dias) if f else "Sin fecha registrada",
        })
    return out


def peor_estado(documentos):
    """El estado que manda sobre los 4 documentos: vencido > por_vencer > incompleto (falta alguna fecha) > vigente."""
    estados = {d["estado"] for d in documentos}
    if "vencido" in estados:
        return "vencido"
    if "por_vencer" in estados:
        return "por_vencer"
    if "sin_fecha" in estados:
        return "incompleto"
    return "vigente"


# ── Validación del vehículo ────────────────────────────────────────────────
_TEXTOS = {          # campo: (largo máximo)
    "marca": 80, "modelo": 80, "color": 40, "combustible": 30,
}


def _txt(valor, maximo, errores, campo):
    t = re.sub(r"\s+", " ", str(valor or "")).strip()
    if len(t) > maximo:
        errores[campo] = f"Máximo {maximo} caracteres."
    return t or None


def validar_vehiculo(payload, hoy=None):
    """Normaliza lo que llega del formulario. Devuelve (datos, errores): `datos` trae TODAS las columnas de COLUMNAS (None si va vacío) y
    `errores` es {campo: mensaje} (vacío = todo bien). Solo patente, tipo y la clase de licencia son obligatorias."""
    p = payload or {}
    hoy = hoy or date.today()
    errores = {}
    datos = {}

    patente = normalizar_patente(p.get("patente"))
    if not patente:
        errores["patente"] = "La patente es obligatoria."
    elif not patente_valida(patente):
        errores["patente"] = "La patente debe tener entre 5 y 7 letras y números (por ejemplo ABCD12 o AB1234)."
    datos["patente"] = patente or None

    tipo = re.sub(r"\s+", " ", str(p.get("tipo") or "")).strip()
    if not tipo:
        errores["tipo"] = "Indica qué tipo de vehículo es (por ejemplo Furgón o Camión)."
    elif len(tipo) < 2 or len(tipo) > 60:
        errores["tipo"] = "El tipo debe tener entre 2 y 60 caracteres."
    datos["tipo"] = tipo or None

    for campo, maximo in _TEXTOS.items():
        datos[campo] = _txt(p.get(campo), maximo, errores, campo)

    vin = re.sub(r"[^A-Z0-9]", "", str(p.get("vin") or "").upper())
    if len(vin) > 40:
        errores["vin"] = "Máximo 40 caracteres."
    datos["vin"] = vin or None

    notas = str(p.get("notas") or "").strip()
    if len(notas) > 1000:
        errores["notas"] = "Máximo 1.000 caracteres."
    datos["notas"] = notas or None

    for campo, minimo, maximo, tipo_num in (
        ("anio", 1950, hoy.year + 1, "int"),
        ("capacidad_kg", 0, 100000, "int"),
        ("estanque_l", 0, 2000, "int"),
        ("km_actual", 0, 3000000, "int"),
        ("capacidad_m3", 0, 200, "dec"),
    ):
        try:
            if tipo_num == "int":
                datos[campo] = _entero(p.get(campo), minimo, maximo, miles=(campo != "anio"))
            else:
                datos[campo] = _decimal(p.get(campo), minimo, maximo)
        except ValueError as e:
            errores[campo] = str(e)
            datos[campo] = None

    clases = normalizar_clases(p.get("licencia_clases"))
    if not clases:
        errores["licencia_clases"] = "Elige al menos una clase de licencia (por ejemplo B)."
    datos["licencia_clases"] = ",".join(clases) if clases else None

    for clave, _nombre in DOCUMENTOS:
        campo = clave + "_vence"
        try:
            datos[campo] = fecha_o_none(p.get(campo))
        except ValueError as e:
            errores[campo] = str(e)
            datos[campo] = None

    return datos, errores


def validar_motivo(texto):
    """(motivo, error) para inhabilitar un vehículo: obligatorio, entre 5 y 300 caracteres."""
    t = re.sub(r"\s+", " ", str(texto or "")).strip()
    if len(t) < MOTIVO_MIN:
        return None, f"Cuenta brevemente por qué (mínimo {MOTIVO_MIN} caracteres)."
    if len(t) > MOTIVO_MAX:
        return None, f"Máximo {MOTIVO_MAX} caracteres."
    return t, None


def _como_texto(v):
    if v is None:
        return ""
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, datetime):
        v = v.date()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def cambios(antes, despues, campos=COLUMNAS):
    """{campo: {'antes': ..., 'despues': ...}} solo de lo que cambió, comparando como texto (un DECIMAL 3.00 y un 3 son lo mismo)."""
    out = {}
    for c in campos:
        a, d = _como_texto((antes or {}).get(c)), _como_texto((despues or {}).get(c))
        if a != d:
            out[c] = {"antes": a or None, "despues": d or None}
    return out


def lineas_de_cambios(cambios_):
    """Los cambios dichos en una línea cada uno, para la bitácora: 'Kilómetros: 12000 → 12500'."""
    out = []
    for c, v in (cambios_ or {}).items():
        antes = v.get("antes") or "(vacío)"
        despues = v.get("despues") or "(vacío)"
        out.append(f"{ETIQUETAS.get(c, c)}: {antes} → {despues}")
    return out

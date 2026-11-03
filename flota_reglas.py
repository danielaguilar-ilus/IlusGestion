"""Reglas PURAS de la flota de ILUS Fitness (vehículos propios, choferes y solicitudes de uso).

Sin Flask, sin base de datos y sin red: todo lo que DECIDE algo vive acá y se prueba en tests/test_flota_vehiculos.py.
flota_module.py solo lee, escribe y pinta.

Pedido de Daniel (02-nov-2026): «registrar los datos de los camiones, tenerlos habilitados y poder habilitar uno nuevo». La flota NO se
fija en el código: hoy son un furgón y un camión, pero cada vehículo nuevo se ingresa a mano desde la pantalla. El sistema sugiere (tipos,
combustibles, clase de licencia) y la persona decide (REGLA #32). Este módulo no maneja precios ni costos.
"""
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import usuarios_perfil as _up      # dirección validada con Google: la MISMA regla de Mi cuenta y Nuevo/Editar usuario

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
COMBUSTIBLES_SUGERIDOS = ("Bencina", "Diésel", "Híbrido", "Eléctrico", "Gas")   # botones del formulario (Daniel, 02-nov-2026)

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
    "patente", "patente_dv", "tipo", "marca", "modelo", "anio", "color", "vin", "numero_motor", "cilindrada",
    "propietario", "propietario_rut",
    "capacidad_kg", "capacidad_m3", "combustible", "estanque_l", "km_actual",
    "licencia_clases",
    "revision_tecnica_vence", "permiso_circulacion_vence", "soap_vence", "seguro_vence",
    "revision_tecnica_detalle", "permiso_circulacion_detalle", "soap_detalle", "seguro_detalle",
    "notas",
)

ETIQUETAS = {
    "patente": "Patente", "tipo": "Tipo", "marca": "Marca", "modelo": "Modelo", "anio": "Año", "color": "Color", "vin": "N° de chasis",
    "capacidad_kg": "Capacidad de carga (kg)", "capacidad_m3": "Volumen (m³)", "combustible": "Combustible",
    "estanque_l": "Estanque (litros)", "km_actual": "Kilómetros", "licencia_clases": "Licencia que exige",
    "revision_tecnica_vence": "Revisión técnica vence", "permiso_circulacion_vence": "Permiso de circulación vence",
    "soap_vence": "SOAP vence", "seguro_vence": "Seguro vence", "notas": "Observaciones",
    "patente_dv": "Dígito verificador de la patente", "numero_motor": "N° de motor", "cilindrada": "Cilindrada / motor",
    "propietario": "Propietario registrado", "propietario_rut": "RUT del propietario",
    "revision_tecnica_detalle": "Revisión técnica · detalle", "permiso_circulacion_detalle": "Permiso de circulación · detalle",
    "soap_detalle": "SOAP · póliza o detalle", "seguro_detalle": "Seguro · póliza o detalle",
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
    "marca": 80, "modelo": 80, "color": 40, "combustible": 30, "propietario": 160, "cilindrada": 20,
    "revision_tecnica_detalle": 160, "permiso_circulacion_detalle": 160, "soap_detalle": 160, "seguro_detalle": 160,
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

    # La patente puede venir con su dígito verificador («TVDZ88-5»): se guarda aparte, para que no parezca una patente de 7 caracteres.
    patente, dv_en_patente = separar_patente(p.get("patente"))
    dv = re.sub(r"[^0-9K]", "", str(p.get("patente_dv") or "").upper()) or dv_en_patente
    if not patente:
        errores["patente"] = "La patente es obligatoria."
    elif not patente_valida(patente):
        errores["patente"] = "La patente debe tener entre 5 y 7 letras y números (por ejemplo ABCD12 o AB1234)."
    elif dv and not re.fullmatch(r"[0-9K]", dv):
        errores["patente"] = "El dígito verificador es un número o la letra K (por ejemplo TVDZ88-5)."
    datos["patente"] = patente or None
    datos["patente_dv"] = dv if dv and re.fullmatch(r"[0-9K]", dv) else None

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
    motor = re.sub(r"[^A-Z0-9]", "", str(p.get("numero_motor") or "").upper())
    if len(motor) > 40:
        errores["numero_motor"] = "Máximo 40 caracteres."
    datos["numero_motor"] = motor or None
    rut_prop = str(p.get("propietario_rut") or "").strip()
    if rut_prop and not rut_valido(rut_prop):
        errores["propietario_rut"] = "El RUT no es válido (revisa el dígito verificador)."
    datos["propietario_rut"] = normalizar_rut(rut_prop) if rut_prop and rut_valido(rut_prop) else None

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


# ═══════════════════════════════════════════════════════════════════════════
#  FASE 1b · CHOFERES Y SOLICITUDES DE USO
#  Daniel (02-nov-2026): «la solicitud la va a poder hacer logística o servicio técnico, asignando el chofer que va a ir a retirar la
#  llave. Aarón marca que la llave fue entregada tal y tal hora». «El sello decide» cuando dos personas piden lo mismo: no hay quien
#  apruebe un choque, el sistema lo dice y se elige otro horario u otro vehículo. Es una RESERVA: no maneja precios ni costos.
# ═══════════════════════════════════════════════════════════════════════════
ESTADOS = {
    "solicitada": "Por confirmar",
    "confirmada": "Confirmada",
    "llave_entregada": "Llave entregada",
    "devuelta": "Devuelta",
    "cancelada": "Cancelada",
}
ESTADOS_OCUPAN = ("solicitada", "confirmada", "llave_entregada")     # los que bloquean el calendario del vehículo
ESTADOS_EDITABLES = ("solicitada", "confirmada")                      # con la llave ya entregada no se mueve (se cancela con motivo)
AREAS_SUGERIDAS = ("Logística", "Servicio Técnico", "Gerencia", "Marketing")   # tarjetas del formulario (Daniel, 02-nov-2026); «Otra área» deja escribir
MAX_DIAS_USO = 7
USO_MOTIVO_MIN, USO_MOTIVO_MAX = 5, 500
CAMBIO_MOTIVO_MIN = 5

DIAS_SEMANA = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
MESES = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")


def _id(valor):
    """Entero positivo o None (acepta 3 o '3')."""
    if isinstance(valor, bool) or valor is None:
        return None
    try:
        n = int(str(valor).strip())
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def parse_hora(valor):
    """'8:30' / '08:30' / '08:30:00' → time. None si viene vacía. ValueError si no es una hora."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return None
    if isinstance(valor, time):
        return valor.replace(second=0, microsecond=0)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", str(valor).strip())
    if not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
        raise ValueError("Hora no válida (usa el formato 08:30).")
    return time(int(m.group(1)), int(m.group(2)))


def hora_texto(dt_):
    return dt_.strftime("%H:%M") if dt_ else ""


def fecha_dia_texto(f):
    """date/datetime → 'lun 03/11/2026' (día de la semana en español, nunca ISO crudo: REGLA #6)."""
    if isinstance(f, datetime):
        f = f.date()
    return f"{DIAS_SEMANA[f.weekday()]} {f.strftime('%d/%m/%Y')}" if f else ""


def rango_texto(inicio, fin):
    """El horario dicho como lo diría una persona: 'lun 03/11/2026 · 08:00 a 12:30' o, si cruza de día,
    'lun 03/11/2026 08:00 → mar 04/11/2026 12:30'."""
    if not inicio or not fin:
        return ""
    if inicio.date() == fin.date():
        return f"{fecha_dia_texto(inicio)} · {hora_texto(inicio)} a {hora_texto(fin)}"
    return f"{fecha_dia_texto(inicio)} {hora_texto(inicio)} → {fecha_dia_texto(fin)} {hora_texto(fin)}"


def numero_solicitud(sid, anio):
    """VEH-2026-0042. Sale del id (único y sin carreras), no de un contador aparte."""
    return f"VEH-{int(anio)}-{int(sid):04d}"


# ── Validación de la solicitud ─────────────────────────────────────────────
def primer_horario(ahora, horas):
    """El primer bloque de media hora que cumple la anticipación mínima: ahora + horas, redondeado hacia arriba a :00 o :30."""
    t = ahora + timedelta(hours=horas)
    base = t.replace(minute=0, second=0, microsecond=0)
    if t > base:
        base += timedelta(minutes=30 if t <= base + timedelta(minutes=30) else 60)
    return base


def validar_solicitud(payload, ahora=None, editando=False, antelacion_horas=None):
    """Normaliza lo que llega del formulario de solicitud. Devuelve (datos, errores).
    `ahora` = la hora de Chile (naive). Con `editando=True` (la gestión mueve una reserva) un uso que ya empezó puede seguir, pero uno que
    ya terminó no. El horario es OBLIGATORIO: «necesito que me declaren el horario en que necesitan el camión»."""
    p = payload or {}
    ahora = ahora or datetime.now()
    hoy = ahora.date()
    errores, datos = {}, {}

    for campo, msg in (("vehiculo_id", "Elige el vehículo."), ("chofer_id", "Elige el chofer que retirará la llave.")):
        n = _id(p.get(campo))
        if n is None:
            errores[campo] = msg
        datos[campo] = n

    area = re.sub(r"\s+", " ", str(p.get("area") or "")).strip()
    if not area:
        errores["area"] = "Indica desde qué área lo pides (por ejemplo Logística o Servicio Técnico)."
    elif len(area) < 2 or len(area) > 60:
        errores["area"] = "El área debe tener entre 2 y 60 caracteres."
    datos["area"] = area or None

    motivo = re.sub(r"\s+", " ", str(p.get("motivo") or "")).strip()
    if len(motivo) < USO_MOTIVO_MIN:
        errores["motivo"] = f"Cuenta para qué lo necesitas (mínimo {USO_MOTIVO_MIN} caracteres)."
    elif len(motivo) > USO_MOTIVO_MAX:
        errores["motivo"] = f"Máximo {USO_MOTIVO_MAX} caracteres."
    datos["motivo"] = motivo or None

    f = fh = hd = hh = None
    try:
        f = fecha_o_none(p.get("fecha"))
    except ValueError as e:
        errores["fecha"] = str(e)
    if f is None and "fecha" not in errores:
        errores["fecha"] = "Elige el día."
    try:
        fh = fecha_o_none(p.get("fecha_hasta")) or f
    except ValueError as e:
        errores["fecha_hasta"] = str(e)
    for campo, msg in (("hora_desde", "Indica a qué hora lo necesitas."), ("hora_hasta", "Indica hasta qué hora lo usarás.")):
        try:
            h = parse_hora(p.get(campo))
        except ValueError as e:
            errores[campo] = str(e)
            h = None
        if h is None and campo not in errores:
            errores[campo] = msg
        if campo == "hora_desde":
            hd = h
        else:
            hh = h

    datos["inicio"] = datos["fin"] = None
    if f and fh and hd and hh and "fecha_hasta" not in errores:
        inicio, fin = datetime.combine(f, hd), datetime.combine(fh, hh)
        datos["inicio"], datos["fin"] = inicio, fin
        if not editando and f < hoy:
            errores["fecha"] = "Ese día ya pasó. Elige hoy o una fecha futura."
        elif fin <= inicio:
            errores["hora_hasta"] = "La hora de término debe ser después de la de inicio."
        elif fin <= ahora:
            errores["hora_hasta"] = "Ese horario ya terminó. Elige uno que no haya pasado."
        elif antelacion_horas and not editando and inicio < ahora + timedelta(hours=antelacion_horas):
            p = primer_horario(ahora, antelacion_horas)
            errores["hora_desde"] = (f"Se pide con al menos {antelacion_horas} horas de anticipación: el primer horario disponible es "
                                    f"{fecha_dia_texto(p.date())} {p.strftime('%H:%M')}.")
        elif fin - inicio > timedelta(days=MAX_DIAS_USO):
            errores["hora_hasta"] = f"Una solicitud puede durar hasta {MAX_DIAS_USO} días."
    return datos, errores


def validar_motivo_cambio(texto, minimo=CAMBIO_MOTIVO_MIN):
    """(motivo, error) del porqué se mueve o se cancela una reserva: obligatorio, queda en la bitácora."""
    t = re.sub(r"\s+", " ", str(texto or "")).strip()
    if len(t) < minimo:
        return None, f"Cuenta brevemente por qué (mínimo {minimo} caracteres)."
    if len(t) > MOTIVO_MAX:
        return None, f"Máximo {MOTIVO_MAX} caracteres."
    return t, None


# ── Choques de horario: el SELLO decide ────────────────────────────────────
def se_cruzan(a_ini, a_fin, b_ini, b_fin):
    """Dos horarios se pisan solo si cada uno empieza antes de que termine el otro. Terminar justo cuando empieza otro NO es un choque."""
    return a_ini < b_fin and a_fin > b_ini


def conflictos(inicio, fin, filas, excluir_id=None):
    """Las reservas de `filas` (con 'inicio', 'fin', 'estado', 'id') que se pisan con [inicio, fin). Solo cuentan las que ocupan el calendario
    (por confirmar, confirmadas y con la llave entregada); una cancelada libera el horario."""
    out = []
    for f in filas or []:
        if excluir_id is not None and f.get("id") == excluir_id:
            continue
        if f.get("estado") not in ESTADOS_OCUPAN:
            continue
        if se_cruzan(inicio, fin, f["inicio"], f["fin"]):
            out.append(f)
    return sorted(out, key=lambda x: (x["inicio"], x.get("id") or 0))


# ── Chofer y vehículo para una solicitud ───────────────────────────────────
def licencia_cubre(clases_chofer, clases_vehiculo):
    """El vehículo exige UNA de sus clases («Clase B o A4»): basta con que el chofer tenga alguna. Si el vehículo no exige ninguna, cubre."""
    exigidas = set(normalizar_clases(clases_vehiculo))
    if not exigidas:
        return True
    return bool(exigidas & set(normalizar_clases(clases_chofer)))


def evaluar_chofer(chofer, clases_vehiculo, fecha_fin, hoy):
    """(error, aviso) de poner a este chofer en este vehículo hasta `fecha_fin`.
    Bloquea lo que sería manejar sin licencia: chofer inhabilitado, sin la clase que exige el vehículo, sin vencimiento registrado o con la
    licencia vencida antes de terminar el uso. Avisa (sin bloquear) si le vence pronto."""
    nombre = (chofer or {}).get("nombre") or "El chofer"
    if not (chofer or {}).get("habilitado"):
        return f"{nombre} está inhabilitado como chofer.", None
    exigidas = normalizar_clases(clases_vehiculo)
    if not licencia_cubre(chofer.get("licencia_clases"), exigidas):
        return f"{nombre} no tiene registrada la licencia que exige este vehículo (clase {' o '.join(exigidas)}).", None
    try:
        vence = fecha_o_none(chofer.get("licencia_vence"))
    except ValueError:
        vence = None
    if vence is None:
        return f"{nombre} no tiene registrado el vencimiento de su licencia.", None
    if isinstance(fecha_fin, datetime):
        fecha_fin = fecha_fin.date()
    if vence < fecha_fin:
        return f"La licencia de {nombre} vence el {fecha_corta(vence)}, antes de terminar este uso.", None
    if estado_vencimiento(vence, hoy) == "por_vencer":
        return None, f"La licencia de {nombre} {texto_dias((vence - hoy).days).lower()} ({fecha_corta(vence)})."
    return None, None


def avisos_vehiculo(fila_vehiculo, fecha_uso):
    """Documentos del vehículo que estarán vencidos el día del uso. ALERTA, no bloquea (queda pendiente la decisión de Daniel)."""
    out = []
    for clave, nombre in DOCUMENTOS:
        try:
            v = fecha_o_none((fila_vehiculo or {}).get(clave + "_vence"))
        except ValueError:
            v = None
        if v is not None and v < fecha_uso:
            out.append(f"{nombre}: vence el {fecha_corta(v)}, antes del día de uso.")
    return out


# ── Choferes ───────────────────────────────────────────────────────────────
CHOFER_COLUMNAS = ("rut", "nombre_completo", "direccion", "licencia_clases", "licencia_vence", "notas")
CHOFER_ETIQUETAS = {"rut": "RUT", "nombre_completo": "Nombre completo", "direccion": "Dirección",
                    "licencia_clases": "Clases de licencia", "licencia_vence": "Licencia vence", "notas": "Observaciones"}


def validar_telefono(t):
    """Teléfono de un chofer externo: se guardan + y dígitos; entre 8 y 15 dígitos. Devuelve (telefono, error)."""
    txt = str(t or "").strip()
    digitos = re.sub(r"\D", "", txt)
    if not digitos:
        return None, "Indica un teléfono para coordinar la entrega de la llave."
    if not 8 <= len(digitos) <= 15:
        return None, "El teléfono debe tener entre 8 y 15 dígitos (por ejemplo +56 9 1234 5678)."
    return ("+" if txt.startswith("+") else "") + digitos, None


def validar_chofer(payload, con_usuario=True, direccion_actual=None, externo=False):
    """Normaliza el alta o la edición de un chofer. Devuelve (datos, errores). Un chofer es un usuario de ILUS (su nombre y teléfono salen
    del perfil) o, desde el 02-nov-2026, un chofer EXTERNO sin usuario (Daniel: «un chofer que no esté dentro del sistema y que se lleve
    el camión»): entonces su nombre es el nombre completo y el teléfono se guarda acá (obligatorio)."""
    p = payload or {}
    errores, datos = {}, {}
    if externo:
        con_usuario = False
        tel, err_tel = validar_telefono(p.get("telefono"))
        if err_tel:
            errores["telefono"] = err_tel
        datos["telefono"] = tel
    if con_usuario:
        uid = _id(p.get("usuario_id"))
        if uid is None:
            errores["usuario_id"] = "Elige a qué usuario de ILUS corresponde este chofer."
        datos["usuario_id"] = uid
    # RUT, nombre completo y dirección (Daniel, 02-nov-2026): lo que dice la licencia. El RUT debe existir (dígito verificador).
    rut_txt = str(p.get("rut") or "").strip()
    if not rut_txt:
        errores["rut"] = "Indica el RUT del chofer."
    elif not rut_valido(rut_txt):
        errores["rut"] = "El RUT no es válido (revisa el dígito verificador)."
    datos["rut"] = normalizar_rut(rut_txt) if rut_txt and rut_valido(rut_txt) else None
    nombre = re.sub(r"\s+", " ", str(p.get("nombre_completo") or "")).strip()
    if len(nombre) < 5:
        errores["nombre_completo"] = "Escribe el nombre completo, como figura en la licencia."
    elif len(nombre) > 190:
        errores["nombre_completo"] = "Máximo 190 caracteres."
    datos["nombre_completo"] = nombre or None
    # Dirección validada con Google (Daniel, 02-nov-2026: «la dirección debe estar validada por Google»), con la MISMA regla que Mi cuenta y
    # Nuevo/Editar usuario (usuarios_perfil.resolver_direccion): es opcional, pero si se escribe tiene que venir elegida de la lista de Google
    # (coordenadas y place_id en Chile) o ser la misma que ya estaba guardada (no se toca su ubicación).
    ok_dir, dir_val = _up.resolver_direccion(p.get("direccion"), p.get("direccion_lat"), p.get("direccion_lng"),
                                             p.get("direccion_place_id"), direccion_actual=direccion_actual)
    if not ok_dir:
        errores["direccion"] = dir_val
        dir_val = {"accion": "mantener", "direccion": None, "lat": None, "lng": None, "place_id": None}
    elif dir_val["accion"] == "limpiar":
        # Obligatoria (Daniel, 02-nov-2026: «colócalo obligatorio»): sin dirección no se guarda el chofer.
        errores["direccion"] = "Indica la dirección del chofer: escríbela y elige una sugerencia de Google."
    datos["direccion"] = dir_val["direccion"]
    datos["direccion_lat"], datos["direccion_lng"], datos["direccion_place_id"] = dir_val["lat"], dir_val["lng"], dir_val["place_id"]
    datos["direccion_accion"] = dir_val["accion"]
    clases = normalizar_clases(p.get("licencia_clases"))
    if not clases:
        errores["licencia_clases"] = "Elige al menos una clase de licencia (por ejemplo B)."
    datos["licencia_clases"] = ",".join(clases) if clases else None
    try:
        vence = fecha_o_none(p.get("licencia_vence"))
    except ValueError as e:
        errores["licencia_vence"] = str(e)
        vence = None
    if vence is None and "licencia_vence" not in errores:
        errores["licencia_vence"] = "Indica cuándo vence la licencia: sin esa fecha no se puede comprobar que esté vigente."
    datos["licencia_vence"] = vence
    notas = str(p.get("notas") or "").strip()
    if len(notas) > 500:
        errores["notas"] = "Máximo 500 caracteres."
    datos["notas"] = notas or None
    return datos, errores


def lineas_de_cambios_chofer(cambios_):
    out = []
    for c, v in (cambios_ or {}).items():
        out.append(f"{CHOFER_ETIQUETAS.get(c, c)}: {v.get('antes') or '(vacío)'} → {v.get('despues') or '(vacío)'}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  FASE 1c · DOCUMENTOS CON PDF, FOTO, DATOS DEL TITULAR Y DE LOS CHOFERES, PERMISOS
#  Daniel (02-nov-2026): «más datos… una opción de dejar el PDF y que no todos tengan acceso a esto»; «todo eso (revisión técnica,
#  permiso, SOAP, seguro) tiene que estar registrado y almacenado, para tener al día»; «déjalo habilitado por el front… nadie puede
#  agregar vehículos ni choferes, solamente gestionar solicitudes»; «yo le dejaría una foto, haz la tabla bien inteligente y con
#  calidad de información».
# ═══════════════════════════════════════════════════════════════════════════
# Fotos de la devolución (Daniel: «declaren la gasolina con una foto… le tomen una foto al odómetro… una foto interna y una externa»).
FOTOS_DEVOLUCION = (
    ("dev_odometro", "Odómetro"),
    ("dev_combustible", "Indicador de combustible"),
    ("dev_interior", "Interior"),
    ("dev_exterior", "Exterior"),
)
TIPOS_ARCHIVO = {
    "vehiculo": ("foto",) + tuple(c for c, _n in DOCUMENTOS),
    "chofer": ("licencia",),
    "solicitud": tuple(c for c, _n in FOTOS_DEVOLUCION),
}
ARCHIVO_MAX_BYTES = {"imagen": 10 * 1024 * 1024, "pdf": 15 * 1024 * 1024}
ETIQUETAS_ARCHIVO = {"foto": "Foto", "licencia": "Licencia de conducir", **{c: n for c, n in DOCUMENTOS},
                     **{c: "Devolución · " + n for c, n in FOTOS_DEVOLUCION}}


def niveles(permisos):
    """Qué puede hacer quien mira la pantalla, a partir de g.permissions. UNA sola función decide los cuatro niveles:
    admin = agregar, editar, archivar y borrar vehículos y choferes y subir sus archivos (el superadministrador, o quien tenga la llave
    flota_administrar: Daniel la enciende por rol o por persona en el front);
    gestionar = confirmar, entregar la llave, mover y cancelar solicitudes de cualquiera (flota_gestionar);
    documentos = ver los PDF y el RUT y la dirección de los choferes (flota_documentos; quien administra también los ve: no se puede
    mantener lo que no se ve);
    solicitar = entrar, ver la flota y pedir un vehículo (flota_solicitar; quien gestiona o administra también entra).
    Administrar NO da gestionar: no confirma ni entrega la llave, y sigue pidiendo con las 24 horas de aviso."""
    p = permisos or {}
    superadmin = bool(p.get("superadmin"))
    admin = superadmin or bool(p.get("flota_administrar"))
    gestionar = superadmin or bool(p.get("flota_gestionar"))
    return {"admin": admin, "gestionar": gestionar,
            "documentos": admin or bool(p.get("flota_documentos")),
            "solicitar": gestionar or admin or bool(p.get("flota_solicitar"))}


# ── Patente con dígito verificador (TVDZ88-5) ──────────────────────────────
_PAT_DV = re.compile(r"^([A-Z]{2,4}[\s.\-]*\d{2,4})\s*-\s*([0-9K])$")


def separar_patente(raw):
    """'TVDZ88-5' / 'PHVD.21-7' → ('TVDZ88', '5') / ('PHVD21', '7'). Sin dígito verificador queda ('AB1234', None). Los documentos chilenos
    escriben la patente con su dígito (PHVD21-7); si se guardara pegado ('PHVD217') parecería una patente de 7 caracteres."""
    up = str(raw or "").upper().strip()
    m = _PAT_DV.match(up)
    if m:
        return normalizar_patente(m.group(1)), m.group(2)
    return normalizar_patente(up), None


def patente_completa(patente, dv):
    return f"{patente}-{dv}" if patente and dv else (patente or "")


# ── RUT ─────────────────────────────────────────────────────────────────────
def normalizar_rut(raw):
    """'12.345.678-5' / '123456785' → '12345678-5'. None si no tiene forma de RUT (7 u 8 dígitos y el verificador)."""
    t = re.sub(r"[^0-9Kk]", "", str(raw or "")).upper()
    if not re.fullmatch(r"\d{7,8}[0-9K]", t):
        return None
    return t[:-1] + "-" + t[-1]


def rut_valido(raw):
    """True si el RUT existe: tiene su forma y el dígito verificador cuadra (módulo 11)."""
    n = normalizar_rut(raw)
    if not n:
        return False
    cuerpo, dv = n.split("-")
    suma, factor = 0, 2
    for d in reversed(cuerpo):
        suma += int(d) * factor
        factor = 2 if factor == 7 else factor + 1
    esperado = 11 - (suma % 11)
    esperado = "0" if esperado == 11 else ("K" if esperado == 10 else str(esperado))
    return dv == esperado


def rut_formato(raw):
    """12345678-5 → 12.345.678-5 (REGLA #7). Si no tiene forma de RUT, devuelve el texto tal cual."""
    n = normalizar_rut(raw)
    if not n:
        return str(raw or "")
    cuerpo, dv = n.split("-")
    return f"{int(cuerpo):,}".replace(",", ".") + "-" + dv


# ── Archivos (foto y PDF) ───────────────────────────────────────────────────
def detectar_archivo(data):
    """Qué es lo que llegó, mirando los primeros bytes (no el nombre ni lo que dice el navegador): ('pdf'|'imagen', content_type, extensión)
    o None. Solo PDF, JPEG, PNG y WebP."""
    b = bytes(data[:16]) if data else b""
    if b.startswith(b"%PDF-"):
        return "pdf", "application/pdf", ".pdf"
    if b.startswith(b"\xff\xd8\xff"):
        return "imagen", "image/jpeg", ".jpg"
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "imagen", "image/png", ".png"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "imagen", "image/webp", ".webp"
    return None


def validar_archivo(entidad, tipo, data):
    """(info, error) de un archivo que se quiere guardar. La foto de un vehículo es una imagen; los documentos del vehículo van en PDF; la
    licencia de un chofer puede ser PDF o la foto del carnet. Sin tope absurdo: 10 MB la imagen, 15 MB el PDF."""
    if tipo not in TIPOS_ARCHIVO.get(entidad, ()):
        return None, "Ese tipo de archivo no corresponde."
    if not data:
        return None, "El archivo llegó vacío."
    det = detectar_archivo(data)
    if det is None:
        return None, "Solo se aceptan PDF (y fotos JPG, PNG o WebP para la foto)."
    clase, ct, ext = det
    if tipo == "foto" and clase != "imagen":
        return None, "La foto del vehículo debe ser una imagen (JPG, PNG o WebP)."
    if tipo.startswith("dev_"):
        if clase != "imagen":
            return None, "Las fotos de la devolución deben ser imágenes (JPG, PNG o WebP)."
    elif tipo != "foto" and tipo != "licencia" and clase != "pdf":
        return None, "Los documentos se guardan en PDF."
    if len(data) > ARCHIVO_MAX_BYTES[clase]:
        return None, f"El archivo pesa demasiado (máximo {ARCHIVO_MAX_BYTES[clase] // (1024 * 1024)} MB)."
    return {"clase": clase, "content_type": ct, "extension": ext, "bytes": len(data)}, None


def nombre_seguro(nombre, extension):
    """Nombre de archivo para mostrar y descargar: sin rutas ni caracteres raros."""
    base = re.sub(r"[^\w.\- ]+", "_", str(nombre or "").rsplit("/", 1)[-1].rsplit("\\", 1)[-1]).strip(" ._")[:80]
    return base if base.lower().endswith(extension) else (base or "archivo") + extension


# ── Calidad de la ficha ─────────────────────────────────────────────────────
def calidad_ficha(fila, tiene_pdf, tiene_foto):
    """Qué tan al día está la información de un vehículo: foto, titular, chasis, N° de motor y, por cada uno de los 4 documentos, la
    fecha de vencimiento Y el PDF guardado. Devuelve {'pct', 'hechos', 'total', 'faltan': [texto…]}: lo que falta dicho en claro, para que
    la tabla le diga a la persona qué completar y no solo un porcentaje."""
    f = fila or {}
    items = [("Foto del vehículo", bool(tiene_foto)),
             ("Propietario registrado", bool(f.get("propietario"))),
             ("N° de chasis (VIN)", bool(f.get("vin"))),
             ("N° de motor", bool(f.get("numero_motor")))]
    for clave, nombre in DOCUMENTOS:
        items.append((f"{nombre}: fecha de vencimiento", bool(f.get(clave + "_vence"))))
        items.append((f"{nombre}: PDF", bool((tiene_pdf or {}).get(clave))))
    hechos = sum(1 for _n, ok in items if ok)
    return {"pct": round(hechos * 100 / len(items)), "hechos": hechos, "total": len(items),
            "faltan": [n for n, ok in items if not ok]}


def proximo_vencimiento(documentos):
    """El documento con fecha que vence antes (vencidos incluidos): {'nombre','vence_fmt','texto','estado'} o None. Para decir en la tabla
    'Lo próximo: SOAP · 31/03/2027 (vence en 149 días)' sin obligar a abrir la ficha."""
    con_fecha = [d for d in (documentos or []) if d.get("vence")]
    if not con_fecha:
        return None
    d = min(con_fecha, key=lambda x: x["vence"])
    return {"nombre": d["nombre"], "vence_fmt": d["vence_fmt"], "texto": d["texto"], "estado": d["estado"]}


def coincide_confirmacion(escrito, esperado, es_patente=False):
    """El borrado definitivo pide escribir la patente (o el nombre del chofer): sin mayúsculas, tildes ni signos de más."""
    def n(t):
        t = str(t or "")
        if es_patente:
            return separar_patente(t)[0]
        t = re.sub(r"\s+", " ", t).strip().casefold()
        for a, b in (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("ñ", "n")):
            t = t.replace(a, b)
        return t
    return bool(n(esperado)) and n(escrito) == n(esperado)


def validar_motivo_archivo(texto):
    """Archivar un vehículo o un chofer pide el motivo (queda en la bitácora)."""
    return validar_motivo(texto)


# ── Horario de entrega de llaves y SLA (Daniel, 02-nov-2026) ──────────────
# «El horario es de 8 a 13 y de 14 a las 17, existe la colación; sin embargo es posible que nos vayamos y que no se entregue la llave:
# apliquemos SLA si se atrasan en entregar las llaves». Fuera de este horario se AVISA (no se bloquea); el reloj del SLA cuenta SOLO
# minutos dentro del horario (como el «Sin responder» de Retiros, REGLA #22).
HORARIO_LLAVES = ((8 * 60, 13 * 60), (14 * 60, 17 * 60))      # minutos del día; 13:00–14:00 es colación
HORARIO_TEXTO = "lunes a viernes hábiles de 08:00 a 13:00 y de 14:00 a 17:00 (colación 13:00–14:00)"
SLA_TOLERANCIA_MIN = 15      # hasta 15 min hábiles después de la hora de retiro: «debe entregarse ahora»
SLA_CRITICO_MIN = 60         # desde 60 min hábiles: atraso crítico


def dia_habil(d, feriados=()):
    return d.weekday() < 5 and d.isoformat() not in feriados


def en_horario(momento, feriados=(), es_termino=False):
    """¿Hay quién entregue (o reciba, con es_termino) la llave a esa hora? El término puede caer justo al cierre (13:00 o 17:00)."""
    if momento is None or not dia_habil(momento.date(), feriados):
        return False
    m = momento.hour * 60 + momento.minute
    return any((a < m <= b) if es_termino else (a <= m < b) for a, b in HORARIO_LLAVES)


def minutos_habiles(desde, hasta, feriados=()):
    """Minutos entre dos momentos contando SOLO el horario de llaves de los días hábiles."""
    if desde is None or hasta is None or hasta <= desde:
        return 0
    total, d = 0, desde.date()
    while d <= hasta.date():
        if dia_habil(d, feriados):
            for a, b in HORARIO_LLAVES:
                ini = datetime.combine(d, time(a // 60, a % 60))
                fin = datetime.combine(d, time(b // 60, b % 60))
                x, y = max(ini, desde), min(fin, hasta)
                if y > x:
                    total += int((y - x).total_seconds() // 60)
        d += timedelta(days=1)
    return total


def avisos_horario(inicio, fin, feriados=()):
    """Avisos (no bloquean) cuando el retiro o la devolución de la llave caen fuera del horario."""
    out = []
    if not en_horario(inicio, feriados):
        out.append(f"El retiro ({fecha_dia_texto(inicio.date())} {inicio.strftime('%H:%M')}) queda fuera del horario de entrega de llaves "
                   f"({HORARIO_TEXTO}): podría no haber quién entregue la llave. Coordínalo antes.")
    if not en_horario(fin, feriados, es_termino=True):
        out.append(f"La devolución ({fecha_dia_texto(fin.date())} {fin.strftime('%H:%M')}) queda fuera del horario de llaves: "
                   f"podría no haber quién reciba la llave.")
    return out


def _duracion(m):
    h, mm = divmod(int(m), 60)
    return (f"{h} h {mm} min" if mm else f"{h} h") if h else f"{mm} min"


def sla_llave(estado, inicio, fin, ahora, feriados=()):
    """Estado del plazo de entrega de la llave de una reserva que todavía NO la tiene entregada. None si no corre.
    en_plazo (hasta 15 min hábiles) · atrasada (ámbar) · critica (rojo, 60 min) · sin_entrega (la reserva terminó sin llave)."""
    if estado not in ("solicitada", "confirmada") or inicio is None or ahora is None or ahora < inicio:
        return None
    sin_conf = estado == "solicitada"
    if fin is not None and ahora >= fin:
        return {"estado": "sin_entrega", "minutos": minutos_habiles(inicio, fin, feriados),
                "texto": "Terminó sin registrar la entrega de la llave" + (" (ni se confirmó)" if sin_conf else "")}
    m = minutos_habiles(inicio, ahora, feriados)
    extra = " · aún sin confirmar" if sin_conf else ""
    if m < SLA_TOLERANCIA_MIN:
        return {"estado": "en_plazo", "minutos": m, "texto": "La llave debe entregarse ahora" + extra}
    if m < SLA_CRITICO_MIN:
        return {"estado": "atrasada", "minutos": m, "texto": f"Llave atrasada · {_duracion(m)} hábiles" + extra}
    return {"estado": "critica", "minutos": m, "texto": f"Atraso crítico · {_duracion(m)} hábiles" + extra}


# ── Devolución de la llave (FASE 2, 03-nov-2026) ───────────────────────────
COMBUSTIBLE_NIVELES = (("lleno", "Lleno"), ("3_4", "3/4"), ("1_2", "1/2"), ("1_4", "1/4"), ("reserva", "Reserva"))
KM_MAX = 2_000_000
MIN_ANTELACION_H = 24        # Daniel, 03-nov-2026: «colócale una restricción de 24 horas al calendario» (pide quien solicita; la gestión puede antes)
KM_SALTO_AVISO = 1_500         # más de esto en un solo uso se avisa (no se bloquea: puede ser un viaje largo)


def validar_devolucion(payload, km_previo=None):
    """(datos, errores, avisos) de una devolución. El kilometraje y el combustible son obligatorios; una novedad o daño exige contarla.
    Un kilometraje MENOR al último registrado o un salto muy grande se AVISAN (no se traban): la foto del odómetro es la evidencia."""
    p = payload or {}
    errores, avisos, datos = {}, [], {}
    txt = re.sub(r"[.\s]", "", str(p.get("km") or ""))
    if not txt:
        errores["km"] = "Indica el kilometraje que marca el odómetro al devolverlo."
        km = None
    elif not txt.isdigit() or not 0 <= int(txt) <= KM_MAX:
        errores["km"] = "El kilometraje debe ser un número entero (por ejemplo 56063)."
        km = None
    else:
        km = int(txt)
    datos["km"] = km
    if km is not None and km_previo is not None:
        if km < int(km_previo):
            avisos.append(f"El kilometraje declarado ({km:,} km) es MENOR al último registrado ({int(km_previo):,} km): revisa la foto del odómetro."
                          .replace(",", "."))
        elif km - int(km_previo) > KM_SALTO_AVISO:
            avisos.append(f"Este uso suma {km - int(km_previo):,} km: revisa que el kilometraje esté bien escrito.".replace(",", "."))
    nivel = str(p.get("combustible") or "").strip()
    if nivel not in dict(COMBUSTIBLE_NIVELES):
        errores["combustible"] = "Elige cuánto combustible le queda."
    datos["combustible"] = nivel if nivel in dict(COMBUSTIBLE_NIVELES) else None
    novedad = str(p.get("novedad") or "").strip().lower() in ("1", "true", "si", "sí", "on")
    obs = re.sub(r"\s+", " ", str(p.get("observaciones") or "")).strip()
    if len(obs) > 500:
        errores["observaciones"] = "Máximo 500 caracteres."
    elif novedad and len(obs) < 10:
        errores["observaciones"] = "Cuenta qué pasó (daño, falla, multa, lo que sea): mínimo 10 caracteres."
    datos["novedad"], datos["observaciones"] = novedad, obs or None
    return datos, errores, avisos


def combustible_texto(nivel):
    return dict(COMBUSTIBLE_NIVELES).get(nivel or "", "")

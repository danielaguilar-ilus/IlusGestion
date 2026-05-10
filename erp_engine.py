"""
ERP Engine — Motor unificado para integración con Random ERP.

Diseñado para ser reusable desde TODOS los módulos del sistema:
  - Cubicador (transporte)
  - Asignar y Cotizar (cotizaciones de courier)
  - Retiros (pickups, retiros de productos)
  - Mantenciones (visitas técnicas, gestión de equipos en servicio)
  - Stock / inventario (futuro)
  - Etiquetas (datos de cliente para imprimir)

API pública mínima:

    from erp_engine import get_client, normalize_phone_cl, rut_variants

    client = get_client()
    doc = client.fetch_document("FCV", "10599")
    if doc:
        print(doc["cliente_nombre"], doc["comuna"])
        for ln in doc["lineas_raw"]:
            print(ln["sku"], ln["cantidad"])

Diseño:
  * Funciones puras (sin estado): normalize_phone_cl, format_rut, rut_variants,
    nudo_variants, resolve_comuna, cmen_to_comuna.
  * Clase ERPClient con session reusable, cache, retry y logger.
  * Devuelve dicts JSON-safe (no dataclasses) para máxima compat con Jinja
    y con la API legacy que esperan los módulos existentes.

Author: ILUS Sport and Health Solutions SPA — daniel.aguilar@sphs.cl
"""

from __future__ import annotations

import logging
import re
import threading
import time
import urllib.parse
import urllib.request
import json as _json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, Iterable, Optional


logger = logging.getLogger("erp.engine")
if not logger.handlers:
    # Solo configurar si nadie más lo configuró (el caller manda)
    logger.setLevel(logging.INFO)


# ════════════════════════════════════════════════════════════════════
#  CONSTANTES — códigos de comuna y región del ERP Random
# ════════════════════════════════════════════════════════════════════

# CIEN: código de región de 3 dígitos (ej. "013"=RM, "005"=Valparaíso)
# CMEN: código de 3 chars (ej. "LOB"=Lo Barnechea, "VAL"=Valdivia/Valparaíso)
CMEN_MAP: dict[str, dict[str, str]] = {
    # ── Región Metropolitana (013) — verificados contra ERP Random Sport&Health ──
    "013": {
        "CEI":"Cerrillos",      "CER":"Cerro Navia",     "COL":"Colina",
        "CON":"Conchalí",       "CUR":"Curacaví",        "ELB":"El Bosque",
        "ELM":"El Monte",       "EST":"Estación Central","HUE":"Huechuraba",
        "IND":"Independencia",  "ISL":"Isla de Maipo",   "LAC":"La Cisterna",
        "LAF":"La Florida",     "LAG":"La Granja",       "LAP":"La Pintana",
        "LAR":"La Reina",       "LAM":"Lampa",           "LAS":"Las Condes",
        "LOB":"Lo Barnechea",   "LOE":"Lo Espejo",       "LOP":"Lo Prado",
        "MAC":"Macul",          "MAI":"Maipú",           "MAR":"María Pinto",
        "MEL":"Melipilla",      "PAD":"Padre Hurtado",   "PAI":"Paine",
        "PED":"Pedro Aguirre Cerda",
        "PEF":"Peñaflor",       "PEN":"Peñalolén",        "PEA":"Peñalolén",
        "PIR":"Pirque",         "PRO":"Providencia",     "PUD":"Pudahuel",
        "PUE":"Puente Alto",    "PUA":"Puente Alto",     "QUI":"Quilicura",
        "QNO":"Quinta Normal",  "REC":"Recoleta",        "REN":"Renca",
        "SBE":"San Bernardo",   "SJM":"San José de Maipo",
        "SMI":"San Miguel",     "SPE":"San Pedro",       "SJO":"San Joaquín",
        "SRA":"San Ramón",      "SAN":"Santiago",        "STG":"Santiago",
        "TAL":"Talagante",      "TIL":"Tiltil",          "VIT":"Vitacura",
        "ALH":"Alhué",          "BUI":"Buin",            "CTA":"Calera de Tango",
        "CAL":"Calera de Tango","NUN":"Ñuñoa",
    },
    # ── Valparaíso (005) ─────────────────────────────────────────────
    "005": {
        "VAL":"Valparaíso",  "VDM":"Viña del Mar", "CON":"Concón",      "QUI":"Quilpué",
        "VLA":"Villa Alemana","SAN":"San Antonio",  "QLL":"Quillota",    "LAC":"La Calera",
        "LAN":"Los Andes",   "SFE":"San Felipe",   "LIM":"Limache",     "OLM":"Olmué",
        "CAB":"Cabildo",     "LLI":"La Ligua",     "ZAP":"Zapallar",    "PAP":"Papudo",
        "QTE":"Quintero",    "PCU":"Puchuncaví",   "CAS":"Casablanca",  "SES":"San Esteban",
        "LLY":"Llaillay",    "PUT":"Putaendo",      "SMR":"Santa María", "ALG":"Algarrobo",
        "CTG":"Cartagena",   "SDO":"Santo Domingo", "EQU":"El Quisco",   "ETA":"El Tabo",
        "RIN":"Rinconada",   "CAL":"Calle Larga",  "JSF":"Juan Fernández","IPA":"Isla de Pascua",
    },
    # ── O'Higgins (006) ──────────────────────────────────────────────
    "006": {
        "RAN":"Rancagua",    "GRA":"Graneros",     "MOS":"Mostazal",    "COD":"Codegua",
        "OLI":"Olivar",      "COL":"Coltauco",     "DON":"Doñihue",     "REN":"Rengo",
        "REQ":"Requínoa",    "SFE":"San Fernando", "CHI":"Chimbarongo", "STA":"Santa Cruz",
        "NAN":"Nancagua",    "PAL":"Palmilla",      "PIC":"Pichilemu",   "LOL":"Lolol",
        "MAR":"Marchihue",   "PAR":"Paredones",    "SVC":"San Vicente", "LCA":"Las Cabras",
        "PEU":"Peumo",       "PID":"Pichidegua",   "MAL":"Malloa",      "MCL":"Machalí",
    },
    # ── Maule (007) ──────────────────────────────────────────────────
    "007": {
        "TAL":"Talca",       "CUR":"Curicó",        "LIN":"Linares",     "CON":"Constitución",
        "CAU":"Cauquenes",   "MOL":"Molina",        "TEN":"Teno",        "ROM":"Romeral",
        "HUA":"Hualañé",     "LIC":"Licantén",      "RAU":"Rauco",       "SCL":"San Clemente",
        "PEN":"Pencahue",    "MAU":"Maule",          "EMP":"Empedrado",   "SJV":"San Javier",
        "VLA":"Villa Alegre","YER":"Yerbas Buenas",  "COL":"Colbún",      "LON":"Longaví",
        "PAR":"Parral",       "RET":"Retiro",
    },
    # ── Biobío (008) ─────────────────────────────────────────────────
    "008": {
        "CON":"Concepción",  "TAL":"Talcahuano",   "HUA":"Hualpén",    "SAN":"San Pedro de la Paz",
        "COR":"Coronel",     "LOT":"Lota",          "TOM":"Tomé",        "PEN":"Penco",
        "CHI":"Chiguayante", "HUL":"Hualqui",       "SJU":"Santa Juana", "FLO":"Florida",
        "ARA":"Arauco",      "CAN":"Cañete",        "LEB":"Lebu",        "LOS":"Los Álamos",
        "CRN":"Curanilahue", "LAJ":"Laja",          "NAC":"Nacimiento",  "MUL":"Mulchén",
        "NEG":"Negrete",     "LAA":"Los Ángeles",   "YUM":"Yumbel",      "CAB":"Cabrero",
        "SRO":"San Rosendo", "CHV":"Chillán Viejo", "BUL":"Bulnes",      "SCA":"San Carlos",
        "SFB":"San Fabián",  "SNN":"San Nicolás",   "NIH":"Ninhue",      "COE":"Coelemu",
        "PEM":"Pemuco",      "ELC":"El Carmen",     "PIN":"Pinto",       "COI":"Coihueco",
        "YUN":"Yungay",      "SIG":"San Ignacio",
    },
    # ── Araucanía (009) ──────────────────────────────────────────────
    "009": {
        "TEM":"Temuco",      "PDL":"Padre las Casas","VIL":"Villarrica", "PUC":"Pucón",
        "ANG":"Angol",       "VIC":"Victoria",       "LAU":"Lautaro",     "FRE":"Freire",
        "GOR":"Gorbea",      "LON":"Loncoche",       "CUR":"Curacautín", "MEL":"Melipeuco",
        "CUN":"Cunco",       "VLC":"Vilcún",          "PER":"Perquenco",   "GAL":"Galvarino",
        "COL":"Collipulli",  "ERC":"Ercilla",         "PUR":"Purén",       "TRA":"Traiguén",
        "REN":"Renaico",     "PIT":"Pitrufquén",      "TOL":"Toltén",      "CAR":"Carahue",
        "NEI":"Nueva Imperial","CHO":"Cholchol",      "SAA":"Saavedra",
    },
    # ── Los Ríos (016) ───────────────────────────────────────────────
    "016": {
        "VAL":"Valdivia",    "LUN":"La Unión",       "RBO":"Río Bueno",  "LRA":"Lago Ranco",
        "FUT":"Futrono",     "PAN":"Panguipulli",    "LLA":"Los Lagos",  "COR":"Corral",
        "MAR":"Mariquina",   "LAN":"Lanco",          "MAF":"Máfil",      "PAI":"Paillaco",
    },
    # ── Los Lagos (010) ──────────────────────────────────────────────
    "010": {
        "PMO":"Puerto Montt","PVA":"Puerto Varas",   "OSO":"Osorno",     "CAS":"Castro",
        "ANC":"Ancud",       "QUE":"Quellón",        "CAL":"Calbuco",    "MAU":"Maullín",
        "LMU":"Los Muermos", "FRU":"Frutillar",      "LLA":"Llanquihue", "PUR":"Purranque",
        "POC":"Puerto Octay","FRE":"Fresia",          "SPB":"San Pablo",  "PUY":"Puyehue",
        "RNE":"Río Negro",   "SJC":"San Juan de la Costa",
        "CHA":"Chaitén",     "FUL":"Futaleufú",      "PAL":"Palena",     "HUL":"Hualaihué",
    },
    # ── Aysén (011) ──────────────────────────────────────────────────
    "011": {
        "COY":"Coyhaique",   "PAY":"Puerto Aysén",   "CCH":"Chile Chico","COC":"Cochrane",
        "OHI":"O'Higgins",   "TOR":"Tortel",          "CIS":"Cisnes",     "LVE":"Lago Verde",
        "RIB":"Río Ibáñez",
    },
    # ── Magallanes (012) ─────────────────────────────────────────────
    "012": {
        "PUA":"Punta Arenas","PNA":"Puerto Natales", "POR":"Porvenir",   "PRI":"Primavera",
        "TIM":"Timaukel",    "LBL":"Laguna Blanca",  "RVE":"Río Verde",  "SGR":"San Gregorio",
        "CAH":"Cabo de Hornos",
    },
    # ── Tarapacá (001) ───────────────────────────────────────────────
    "001": {
        "IQU":"Iquique",     "ALH":"Alto Hospicio",  "POZ":"Pozo Almonte","PIC":"Pica",
        "COL":"Colchane",    "CAM":"Camiña",          "HUA":"Huara",
    },
    # ── Arica y Parinacota (015) ─────────────────────────────────────
    "015": {
        "ARI":"Arica",       "CAM":"Camarones",      "PUT":"Putre",       "GLA":"General Lagos",
    },
    # ── Antofagasta (002) ────────────────────────────────────────────
    "002": {
        "ANT":"Antofagasta", "CAL":"Calama",          "TOC":"Tocopilla",  "MEJ":"Mejillones",
        "TAL":"Taltal",       "SPA":"San Pedro de Atacama","OLL":"Ollagüe","MRE":"María Elena",
    },
    # ── Atacama (003) ────────────────────────────────────────────────
    "003": {
        "COP":"Copiapó",     "CLD":"Caldera",         "CHA":"Chañaral",  "DIA":"Diego de Almagro",
        "VAL":"Vallenar",    "FRE":"Freirina",         "HUA":"Huasco",    "ALC":"Alto del Carmen",
        "TIA":"Tierra Amarilla",
    },
    # ── Coquimbo (004) ───────────────────────────────────────────────
    "004": {
        "LSE":"La Serena",   "COQ":"Coquimbo",        "OVA":"Ovalle",    "ILL":"Illapel",
        "LVI":"Los Vilos",   "SAL":"Salamanca",        "CAN":"Canela",    "MPT":"Monte Patria",
        "PUN":"Punitaqui",   "VIC":"Vicuña",           "ANT":"Andacollo", "PAI":"Paihuano",
        "COM":"Combarbalá",  "LHG":"La Higuera",
    },
}

REGION_NOMBRES: dict[str, str] = {
    "001": "Tarapacá",          "002": "Antofagasta",         "003": "Atacama",
    "004": "Coquimbo",          "005": "Valparaíso",          "006": "O'Higgins",
    "007": "Maule",             "008": "Biobío",              "009": "Araucanía",
    "010": "Los Lagos",         "011": "Aysén",               "012": "Magallanes",
    "013": "Metropolitana",     "014": "Los Ríos",            "015": "Arica y Parinacota",
    "016": "Los Ríos",
}

# VD y WEB usan TIDO=NVV en el ERP; el NUDO lleva el prefijo dentro (10 chars).
TIDO_NUDO_MAP: dict[str, tuple[str, Callable[[str], str]]] = {
    "VD":  ("NVV", lambda n: "VD"  + str(n).zfill(8)),
    "WEB": ("NVV", lambda n: "WEB" + str(n).zfill(7)),
}


# ════════════════════════════════════════════════════════════════════
#  FUNCIONES PURAS — utilidades sin estado
# ════════════════════════════════════════════════════════════════════

def normalize_phone_cl(raw: str) -> str:
    """Normaliza teléfono chileno al formato +56XXXXXXXXX (12 chars).

    Casos soportados:
      +56936535760  →  +56936535760  (ya correcto)
      +569 3653 5760 → +56936535760  (espacios)
      56936535760    → +56936535760  (sin +)
      936535760      → +56936535760  (solo 9 dígitos, celular)
      236535760      → +56236535760  (fijo, 8 dígitos con código)
    """
    if not raw:
        return ""
    p = re.sub(r"[\s\-\(\)\.]+", "", str(raw).strip())
    if not p:
        return ""
    if p.startswith("+"):
        digits = p[1:]
        if digits.startswith("56") and len(digits) == 11:
            return "+" + digits
        if digits.startswith("569") and len(digits) == 12:
            return "+56" + digits[2:]
        return p
    if p.startswith("56") and len(p) == 11:
        return "+" + p
    if p.startswith("9") and len(p) == 9:
        return "+56" + p
    if p.startswith("9") and len(p) == 8:
        return "+569" + p
    if p.isdigit() and len(p) == 8:
        return "+562" + p
    return "+56" + p


def _compute_dv(rut_num: str) -> str:
    """Calcula DV chileno por módulo 11 sobre la parte numérica."""
    if not rut_num or not rut_num.isdigit():
        return ""
    reversed_digits = list(map(int, reversed(rut_num)))
    factors = [2, 3, 4, 5, 6, 7]
    total = sum(d * factors[i % 6] for i, d in enumerate(reversed_digits))
    rest = 11 - (total % 11)
    if rest == 11:
        return "0"
    if rest == 10:
        return "K"
    return str(rest)


def format_rut(rut: str, with_dots: bool = False, has_dv: Optional[bool] = None) -> str:
    """Formatea RUT con DV.

    Args:
        rut: RUT con o sin DV, con o sin puntos/guión.
        with_dots: Si True, agrega puntos separadores de miles.
        has_dv: Pista explícita: True=trae DV, False=sin DV, None=autodetectar.
                Autodetección: si trae guión o termina en K, asume CON DV.
                Si son todos dígitos sin guión, asume SIN DV (típico ENDO del ERP).
    """
    if not rut:
        return ""
    raw = str(rut).strip()
    s = raw.replace(".", "").replace("-", "").upper()
    if not s:
        return ""

    if has_dv is None:
        # Autodetectar: con guión o K final → trae DV
        if "-" in raw or s.endswith("K"):
            has_dv = True
        else:
            has_dv = False

    if has_dv and len(s) >= 2:
        num, dv = s[:-1], s[-1]
    else:
        num, dv = s, _compute_dv(s)

    if with_dots and len(num) >= 4:
        parts = []
        for i, c in enumerate(reversed(num)):
            if i and i % 3 == 0:
                parts.append(".")
            parts.append(c)
        num = "".join(reversed(parts))
    return f"{num}-{dv}"


def rut_variants(rut: str) -> list[str]:
    """Devuelve todas las variantes plausibles del RUT para probar contra /entidades.

    Lógica de autodetección de DV:
      - Si trae guión ("12345678-9") o termina en K → asume CON DV
      - Si son todos dígitos sin guión → asume SIN DV (típico ENDO del header ERP)

    Variantes generadas en orden de probabilidad:
      1. Original tal cual (lo que entregó el ERP)
      2. Limpio (sin puntos ni guiones)
      3. Con DV calculado por módulo 11 (formato 12345678-K)
      4. Con DV pegado (12345678K)
      5. Con puntos (12.345.678-K)
      6. Sin DV (1234567 = parte numérica desnuda)
      7. DV alternativos por si el módulo 11 calculado no coincide con el guardado
    """
    if not rut:
        return []
    raw = str(rut).strip()
    clean = raw.replace(".", "").replace("-", "").upper()
    if not clean:
        return []

    # Autodetección: con guión o termina en K → trae DV
    has_dv = ("-" in raw) or clean.endswith("K")
    variants: list[str] = []

    def _add(v: str) -> None:
        if v and v not in variants:
            variants.append(v)

    _add(raw)
    _add(clean)

    if has_dv and len(clean) >= 2:
        num = clean[:-1]
        dv = clean[-1]
        # Variantes con el DV que llegó
        _add(f"{num}-{dv}")
        _add(f"{num}{dv}")
        _add(format_rut(clean, with_dots=True, has_dv=True))
        # Sin DV
        _add(num)
    else:
        # Sin DV — generar variantes con DV calculado y alternativas
        num = clean
        if num.isdigit() and len(num) >= 6:
            dv_correct = _compute_dv(num)
            # El DV calculado va primero (más probable)
            for dv in [dv_correct, "K", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
                if dv:
                    _add(f"{num}-{dv}")
                    _add(f"{num}{dv}")
            # Con puntos (formato canónico)
            if dv_correct:
                _add(format_rut(num + dv_correct, with_dots=True, has_dv=True))

    return variants


def nudo_variants(nudo_raw: str) -> list[str]:
    """El ERP guarda NUDO como string de 10 chars con ceros a la izquierda.
    Devuelve lista de variantes a probar.

    Casos especiales:
      - NV WEB: prefijo "WEB" + 7 dígitos (ej. WEB0021756)
      - NV directa: prefijo "VD" + 8 dígitos (ej. VD00009344)
      - Otros docs: solo zfill a 10
    """
    s = str(nudo_raw).strip()
    if not s:
        return []
    m = re.match(r"^([A-Za-z]*)(\d+)$", s)
    if m:
        prefix, num = m.group(1).upper(), m.group(2)
        variants = [
            prefix + num.zfill(max(0, 10 - len(prefix))),  # padding total 10
            prefix + num.zfill(7),                          # WEB+7 dígitos típico
            prefix + num.zfill(8),                          # VD+8 dígitos
            prefix + num.zfill(6),
            prefix + num,                                   # sin padding
            num.zfill(10),                                  # sin prefijo
            num,                                            # tal cual
        ]
    else:
        variants = [s.zfill(10), s, s.zfill(8), s.zfill(7)]
    # dedup preservando orden
    return list(dict.fromkeys(variants))


def cmen_to_comuna(cien: str, cmen: str) -> str:
    """Convierte (CIEN_región, CMEN_comuna) a nombre de comuna legible.
    Si no hay match, retorna el CMEN tal cual (sirve como seed).
    """
    if not cmen:
        return ""
    region_map = CMEN_MAP.get(str(cien).zfill(3), {})
    nombre = region_map.get(str(cmen).upper().strip())
    if nombre:
        return nombre
    # Fallback: buscar en todas las regiones (por si el CIEN viene mal)
    key = str(cmen).upper().strip()
    for rmap in CMEN_MAP.values():
        if key in rmap:
            return rmap[key]
    return str(cmen)


def resolve_comuna(val: str, prefer_cien: str = "013") -> str:
    """Resuelve un valor que puede ser código (CEI, 3-4 chars mayúsculas)
    o nombre (CERRILLOS) a nombre legible.
    """
    if not val:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    # Si parece código (≤4 chars, mayúsculas, sin números)
    if len(s) <= 4 and s.upper() == s and s.isalpha():
        nombre = cmen_to_comuna(prefer_cien, s)
        if nombre and nombre != s:
            return nombre
        for cien in CMEN_MAP.keys():
            r = cmen_to_comuna(cien, s)
            if r and r != s:
                return r
    # Es nombre — capitalizar si viene en mayúsculas
    return s.title() if s.upper() == s else s


# ════════════════════════════════════════════════════════════════════
#  ERPClient — cliente con sesión HTTP, cache, retry y logger
# ════════════════════════════════════════════════════════════════════

class ERPClient:
    """Cliente reusable para la REST API de Random ERP.

    Maneja:
      - Llamadas GET con auth Bearer
      - Cache de documentos (TTL doc_ttl, default 90s)
      - Cache de entidades (TTL ent_ttl, default 300s)
      - Reintentos automáticos en errores transitorios
      - Búsqueda paralela de variantes de RUT
      - Logger para diagnósticos

    Thread-safe: caches protegidos por lock.
    """

    # Claves del header del documento donde puede venir el RUT del cliente
    HDR_RUT_KEYS = (
        "ENDO", "RTEN", "RUT", "CLEN",
        "ENDODESP", "RTENDESP",
        "RTENDO", "RUTEN", "RUTEMP",
        "RUTCLI", "ENDOFAC", "ENDOEMI",
    )
    # Claves donde puede venir el nombre del cliente — header
    HDR_NAME_KEYS = (
        "NOKOEN", "NRAZON", "NOMENT", "RAZONSOCIAL",
        "NRAZONSOC", "NRAZONFINAL", "RAZON",
        "NOKEN", "NOKO", "NOMBRE", "NOMENDO",
        "NOKOENDESP", "NOMENDESP",  # destinatario (WEB B2C)
    )
    HDR_EMAIL_KEYS = (
        "EMAIL", "EMAILEN", "EMAILCOMER", "MAIL", "MAILEN",
        "CORREO", "EMAILCLI", "EMAILDESP", "MAILCOMERCIAL",
    )
    HDR_PHONE_KEYS = (
        "FOEN", "FONOEN", "TELEFONOEN", "FONO", "TEL",
        "TELEFONO", "FONOMOVIL", "CELULAR", "FAEN",
    )
    HDR_ADDR_KEYS = (
        "DIEN", "DIRECEN", "DIRECCION", "DIENDESP", "DIENDE",
        "DIRENDESP", "DIRECCIONEN", "DIRECCIONDESP",
    )
    HDR_OBS_KEYS = (
        "OBEN", "OBENEN", "OBDO", "OBSERVA", "OBSERVACIONES",
        "OBSCLI", "OBSDOC", "NOTAS", "COMENTARIO", "OBSERVACION",
        "REFERENCIA", "DETOFE",
    )
    # Claves en las LÍNEAS (maeddo) — el ERP Random embebe datos del cliente
    # en las primeras líneas cuando el header no los trae (típico en WEB/NVV)
    LINE_NAME_KEYS = (
        "NOKOEN", "NOKOENDE", "NOMENDE", "RAZSOCDE",
        "NRAZON", "NOMENT", "NOMBRE", "DESCDESP",
    )
    LINE_ADDR_KEYS = ("DIEN", "DIENDESP", "DIENDE", "DIRECCION", "DIRECDESP")
    LINE_COMUNA_KEYS = ("COMUNA", "CMEN", "NOKOCOMU", "NOKOZO")
    LINE_OBS_KEYS = ("OBDO", "OBSERVA", "OBSERVACION", "OBENEN", "OBLI")
    LINE_ZONA_KEYS = ("NOKOZO", "ZONA", "NOKOZONA")

    # ZZ — códigos de SKU que son servicio/flete (no productos físicos)
    ZZ_CODES = frozenset({
        "ZZENVIO", "ZZINGREPUESTO", "ZZSERVTEC",
        "ZZRETIRO", "ZZINSTALACION", "ZZINGARREQUIP",
    })

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        doc_ttl: int = 90,
        ent_ttl: int = 300,
        timeout: int = 6,
        retries: int = 2,
        ext_logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.doc_ttl = doc_ttl
        self.ent_ttl = ent_ttl
        self.timeout = timeout
        self.retries = retries
        self.log = ext_logger or logger

        self._doc_cache: dict[str, tuple[float, dict]] = {}
        self._ent_cache: dict[str, tuple[float, Optional[dict]]] = {}
        self._lock = threading.Lock()

    # ── HTTP ────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict, *, timeout: Optional[int] = None) -> dict:
        """GET autenticado. Lanza ConnectionError si falla todos los retries."""
        url = self.base_url + path
        qs = urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"{url}?{qs}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        last_err = None
        for attempt in range(self.retries + 1):
            try:
                t0 = time.time()
                with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                    body = _json.loads(resp.read().decode("utf-8"))
                    elapsed_ms = int((time.time() - t0) * 1000)
                    self.log.debug("ERP GET %s %s → %d ms", path, params, elapsed_ms)
                    return body
            except Exception as e:
                last_err = e
                if attempt < self.retries:
                    backoff = 0.3 * (2 ** attempt)
                    self.log.debug("ERP GET retry %d (%.1fs) %s: %s",
                                   attempt + 1, backoff, path, e)
                    time.sleep(backoff)
        raise ConnectionError(f"ERP GET {path} falló tras {self.retries+1} intentos: {last_err}")

    # ── Cache helpers ───────────────────────────────────────────────

    def invalidate_doc(self, tido: str, nudo: str) -> None:
        key = f"{tido}|{nudo}"
        with self._lock:
            self._doc_cache.pop(key, None)

    def invalidate_all(self) -> None:
        with self._lock:
            self._doc_cache.clear()
            self._ent_cache.clear()

    def _doc_cached(self, key: str) -> Optional[dict]:
        with self._lock:
            v = self._doc_cache.get(key)
        if v and (time.time() - v[0]) < self.doc_ttl:
            return v[1]
        return None

    def _doc_store(self, key: str, doc: dict) -> None:
        with self._lock:
            self._doc_cache[key] = (time.time(), doc)
            if len(self._doc_cache) > 200:
                cutoff = time.time() - self.doc_ttl
                stale = [k for k, (t, _) in self._doc_cache.items() if t < cutoff]
                for k in stale:
                    self._doc_cache.pop(k, None)

    # ── Búsqueda de entidad (cliente) ───────────────────────────────

    def fetch_entity(self, rut: str) -> Optional[dict]:
        """Busca un cliente en /entidades por RUT, probando variantes en paralelo.
        Devuelve el dict raw del ERP o None si no se encuentra.
        """
        if not rut:
            return None
        cache_key = str(rut).upper().strip()
        with self._lock:
            v = self._ent_cache.get(cache_key)
        if v and (time.time() - v[0]) < self.ent_ttl:
            return v[1]

        variants = rut_variants(rut)
        if not variants:
            return None

        winner = None
        # Limitamos a 6 variantes para evitar saturar el ERP con 12-15 requests
        variants = variants[:6]

        def _try(rv: str) -> Optional[dict]:
            try:
                body = self._get("/entidades", {"rten": rv}, timeout=3)
                data = body.get("data") or []
                if data:
                    self.log.info("ERP entity HIT rten=%s", rv)
                    return data[0]
            except Exception as e:
                self.log.debug("ERP entity miss rten=%s: %s", rv, e)
            return None

        with ThreadPoolExecutor(max_workers=min(4, len(variants))) as pool:
            futures = {pool.submit(_try, rv): rv for rv in variants}
            try:
                for fut in as_completed(futures, timeout=5):
                    result = fut.result()
                    if result:
                        winner = result
                        # Cancel resto (no garantizado pero ayuda)
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
            except Exception as e:
                self.log.warning("ERP entity timeout: %s", e)

        with self._lock:
            self._ent_cache[cache_key] = (time.time(), winner)
            if len(self._ent_cache) > 500:
                cutoff = time.time() - self.ent_ttl
                stale = [k for k, (t, _) in self._ent_cache.items() if t < cutoff]
                for k in stale:
                    self._ent_cache.pop(k, None)
        return winner

    def fetch_entity_by_name(self, name: str) -> Optional[dict]:
        """Búsqueda secundaria por NRAZON cuando el RUT no resuelve.
        Útil para ventas web donde el ENDO es B2C placeholder.
        """
        if not name or len(name.strip()) < 4:
            return None
        try:
            body = self._get("/entidades", {"nrazon": name.strip()}, timeout=4)
            data = body.get("data") or []
            if data:
                self.log.info("ERP entity HIT nrazon=%s", name[:40])
                return data[0]
        except Exception as e:
            self.log.debug("ERP entity miss nrazon=%s: %s", name[:40], e)
        return None

    # ── Búsqueda de documento ───────────────────────────────────────

    @staticmethod
    def _pick(d: dict, keys: Iterable[str]) -> str:
        """Devuelve el primer valor no-vacío entre las keys dadas (case-insensitive)."""
        for k in keys:
            v = d.get(k) or d.get(k.lower()) or d.get(k.upper())
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    @classmethod
    def _scan_lines(cls, lines: list[dict]) -> dict:
        """Extrae datos del cliente embebidos en las líneas.
        Random ERP a veces guarda NOKOEN/DIEN/COMUNA/OBDO en maeddo[0..N].
        """
        out = {"nombre": "", "direccion": "", "comuna": "", "obs": "", "zona": ""}
        if not lines:
            return out
        for ln in lines:
            if not out["nombre"]:
                out["nombre"] = cls._pick(ln, cls.LINE_NAME_KEYS).title()
            if not out["direccion"]:
                out["direccion"] = cls._pick(ln, cls.LINE_ADDR_KEYS).title()
            if not out["comuna"]:
                out["comuna"] = cls._pick(ln, cls.LINE_COMUNA_KEYS)
            if not out["obs"]:
                out["obs"] = cls._pick(ln, cls.LINE_OBS_KEYS)
            if not out["zona"]:
                out["zona"] = cls._pick(ln, cls.LINE_ZONA_KEYS)
        return out

    def fetch_document(self, tido: str, nudo: str) -> Optional[dict]:
        """Recupera un documento del ERP con TODA la info del cliente y líneas.

        Devuelve dict con:
          tido, nudo, nudo_display, erp_tido, erp_nudo, fecha
          cliente_nombre, cliente_rut, email, telefono, direccion, comuna, observaciones
          valor_neto, valor_iva, valor_bruto
          lineas_raw: list[dict] — líneas crudas del ERP (KOPRCT, NOKOPR, CAPRCO1, VANELI, etc.)
          raw_header, raw_lineas: snapshots para diagnóstico
          diagnostics: { nudo_tried, rut_tried, fallback_chain, latency_ms }
        """
        cache_key = f"{tido}|{nudo}"
        cached = self._doc_cached(cache_key)
        if cached:
            return cached

        t0 = time.time()
        # Mapear VD/WEB → TIDO=NVV con NUDO prefijado
        display_tido = tido
        if tido in TIDO_NUDO_MAP:
            erp_tido, nudo_fn = TIDO_NUDO_MAP[tido]
            erp_nudo = nudo_fn(nudo)
        else:
            erp_tido = tido
            erp_nudo = str(nudo).strip()

        nudos = nudo_variants(erp_nudo)
        diag: dict[str, Any] = {
            "nudo_tried": [], "rut_tried": [],
            "fallback_chain": [], "latency_ms": 0,
            "match_nudo": None, "match_rut": None,
        }

        raw_header: dict = {}
        raw_lines: list[dict] = []
        last_err: Optional[Exception] = None
        matched_nudo: Optional[str] = None

        for nv in nudos:
            diag["nudo_tried"].append(nv)
            try:
                body = self._get("/documentos/render",
                                 {"tido": erp_tido, "nudo": nv, "empresa": "01"})
                data = body.get("data") or []
                if data:
                    raw_header = data[0].get("maeedo") or {}
                    raw_lines = data[0].get("maeddo") or []
                    matched_nudo = nv
                    diag["match_nudo"] = nv
                    diag["fallback_chain"].append(f"render OK con nudo={nv}")
                    break
            except Exception as e:
                last_err = e

        if not raw_header:
            diag["latency_ms"] = int((time.time() - t0) * 1000)
            if last_err:
                self.log.warning("ERP doc %s/%s: error final: %s", tido, nudo, last_err)
                raise ConnectionError(f"ERP no respondió ({last_err}). Reintenta.")
            self.log.info("ERP doc %s/%s NO ENCONTRADO (probé %d nudos)",
                          tido, nudo, len(diag["nudo_tried"]))
            return None

        # ── Extraer datos del cliente desde el header ───────────────
        endo = self._pick(raw_header, self.HDR_RUT_KEYS)
        header_nombre = self._pick(raw_header, self.HDR_NAME_KEYS).title()
        header_email = self._pick(raw_header, self.HDR_EMAIL_KEYS)
        header_fono = self._pick(raw_header, self.HDR_PHONE_KEYS)
        header_dir = self._pick(raw_header, self.HDR_ADDR_KEYS).title()
        header_obs = self._pick(raw_header, self.HDR_OBS_KEYS)

        # ── Extraer datos del cliente desde las LÍNEAS (fallback) ──
        line_data = self._scan_lines(raw_lines)
        if line_data["nombre"]:
            diag["fallback_chain"].append(f"nombre desde línea: {line_data['nombre'][:30]}")
        # Aplicar como fallback si el header no los trajo
        nombre_efectivo = header_nombre or line_data["nombre"]
        dir_efectivo = header_dir or line_data["direccion"]
        obs_efectivo = header_obs or line_data["obs"]

        # ── Consultar /entidades para enriquecer datos ──────────────
        cliente_nombre = ""
        cliente_rut = endo
        cliente_email = ""
        cliente_telefono = ""
        cliente_dir_base = ""
        cliente_cmen = ""
        cliente_cien = ""
        cliente_obs = ""
        cliente_comuna_nombre = ""

        ent = None
        if endo:
            ent = self.fetch_entity(endo)
            diag["rut_tried"] = rut_variants(endo)[:6]
            if ent:
                diag["match_rut"] = self._pick(ent, ("RTEN", "ENDO"))
                diag["fallback_chain"].append(f"entidad por RUT: {diag['match_rut']}")

        # Fallback: si no encontró por RUT, intentar por NRAZON con el nombre conocido
        if not ent and nombre_efectivo:
            ent = self.fetch_entity_by_name(nombre_efectivo)
            if ent:
                diag["fallback_chain"].append(f"entidad por NRAZON: {nombre_efectivo[:30]}")

        if ent:
            cliente_nombre = self._pick(ent, ("NOKOEN", "NRAZON", "NOMBRE")).title()
            cliente_rut = self._pick(ent, ("RTEN", "ENDO")) or endo
            cliente_email = self._pick(ent, ("EMAIL", "EMAILCOMER", "MAIL"))
            cliente_telefono = normalize_phone_cl(self._pick(ent, ("FOEN", "FAEN", "FONO", "CELULAR")))
            cliente_dir_base = self._pick(ent, ("DIEN", "DIRECCION", "DIRECEN")).title()
            cliente_cien = self._pick(ent, ("CIEN", "REGION"))
            cliente_cmen = self._pick(ent, ("CMEN", "COMUNA"))
            cliente_obs = self._pick(ent, ("OBEN", "OBSERVACIONES"))
            cliente_comuna_nombre = cmen_to_comuna(cliente_cien, cliente_cmen)

        # ── Consolidar (preferencia: /entidades > header > líneas) ──
        cliente_nombre = cliente_nombre or nombre_efectivo
        cliente_email = cliente_email or header_email
        cliente_telefono = cliente_telefono or normalize_phone_cl(header_fono)
        cliente_dir_final = (
            self._pick(raw_header, ("DIENDESP", "DIENDE"))   # despacho del doc
            or dir_efectivo
            or cliente_dir_base
        )
        cliente_obs = cliente_obs or obs_efectivo

        # Comuna: prioridad doc > línea > entidad > zona
        comuna_doc = self._pick(raw_header,
                                ("NOKOZO", "NOKOCOMU", "NOKOCOMUNADE",
                                 "NOKOMUENDE", "NOKOMUNEN", "NOKCOMENDESP"))
        comuna_final = (
            resolve_comuna(comuna_doc)
            or resolve_comuna(line_data["comuna"])
            or cliente_comuna_nombre
            or resolve_comuna(line_data["zona"])
        )

        # ── Formatear fecha ──────────────────────────────────────────
        fecha_raw = raw_header.get("FEEMDO", "")
        try:
            fecha = datetime.fromisoformat(
                fecha_raw.replace("Z", "+00:00")
            ).strftime("%d/%m/%Y")
        except Exception:
            fecha = fecha_raw

        # ── Diagnostics snapshot ────────────────────────────────────
        raw_sample = {}
        for k, v in (raw_header or {}).items():
            if v is None:
                continue
            sv = str(v).strip()
            if sv and sv not in ("0", "0.0", "0.00", "False"):
                raw_sample[k] = sv[:200]
        raw_line_sample = {}
        if raw_lines:
            for k, v in raw_lines[0].items():
                if v is None:
                    continue
                sv = str(v).strip()
                if sv and sv not in ("0", "0.0", "0.00", "False"):
                    raw_line_sample[k] = sv[:200]

        diag["latency_ms"] = int((time.time() - t0) * 1000)

        doc = {
            # Identificación
            "tido":             display_tido,
            "nudo":             str(nudo),
            "nudo_display":     str(nudo).lstrip("0") or str(nudo),
            "erp_tido":         erp_tido,
            "erp_nudo":         matched_nudo or erp_nudo,
            "fecha":            fecha,
            # Cliente (consolidado)
            "cliente_nombre":   cliente_nombre,
            "cliente_rut":      cliente_rut,
            "email":            cliente_email,
            "telefono":         cliente_telefono,
            "direccion":        cliente_dir_final,
            "comuna":           comuna_final,
            "observaciones":    cliente_obs,
            # Totales
            "valor_neto":       float(raw_header.get("VANEDO") or 0),
            "valor_iva":        float(raw_header.get("VAIVDO") or 0),
            "valor_bruto":      float(raw_header.get("VABRDO") or 0),
            # Líneas crudas (sin cruzar con BD local — eso lo hace el caller)
            "lineas_raw":       raw_lines,
            # Diagnóstico
            "all_fields":       list(raw_header.keys()),
            "raw_sample":       raw_sample,
            "raw_linea_sample": raw_line_sample,
            "n_lineas":         len(raw_lines),
            "datos_completos":  bool(cliente_nombre and (cliente_email or cliente_telefono)),
            "diagnostics":      diag,
        }

        self._doc_store(cache_key, doc)
        self.log.info(
            "ERP doc %s/%s OK en %d ms (cliente=%s, fb=%s)",
            tido, nudo, diag["latency_ms"],
            (cliente_nombre or "?")[:30],
            " > ".join(diag["fallback_chain"]) or "n/a",
        )
        return doc


# ════════════════════════════════════════════════════════════════════
#  Singleton — instancia compartida (configurable via init_engine)
# ════════════════════════════════════════════════════════════════════

_engine: Optional[ERPClient] = None
_engine_lock = threading.Lock()


def init_engine(base_url: str, token: str, **kwargs) -> ERPClient:
    """Inicializa (o re-inicializa) el cliente singleton.
    Llamar UNA VEZ al arranque de la app, después de cargar ERP_CONFIG.
    """
    global _engine
    with _engine_lock:
        _engine = ERPClient(base_url, token, **kwargs)
    return _engine


def get_client() -> ERPClient:
    """Devuelve el cliente singleton. Lanza RuntimeError si no se inicializó."""
    if _engine is None:
        raise RuntimeError(
            "erp_engine no inicializado. Llamar init_engine(base_url, token) primero."
        )
    return _engine

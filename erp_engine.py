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

# ────────────────────────────────────────────────────────────────────
#  CMEN_MAP — Mapa completo extraído de la tabla LLAVE del ERP Random
# ────────────────────────────────────────────────────────────────────
# Estructura: { "<num_region>": { "<codigo>": "<nombre_legible>" } }
#
# IMPORTANTE — Codificación de Ñ:
#   El ERP Random guarda el carácter "Ñ" como "¥" (yen, byte 0xA5) por
#   usar codificación CP437/CP850 antigua. Algunos códigos contienen "¥"
#   literal — ej. "5VI¥" = Viña del Mar, "13¥U¥" = Ñuñoa.
#
#   Mantenemos los códigos con "¥" porque ASÍ se buscan en el ERP, pero
#   los nombres legibles usan Ñ correcta para mostrar al usuario.
#
# Los códigos de región son 1-16 SIN ceros a la izquierda (no "013").
# Tabla verificada con la LLAVE actual de Random (mayo 2026).
# ────────────────────────────────────────────────────────────────────
CMEN_MAP: dict[str, dict[str, str]] = {
    # ── 1. Tarapacá ──────────────────────────────────────────────────
    "1": {
        "ATH": "Alto Hospicio",  "CAI": "Camiña",          "COL": "Colchane",
        "HUA": "Huara",          "IQU": "Iquique",         "PIC": "Pica",
        "POZ": "Pozo Almonte",
    },
    # ── 2. Antofagasta ───────────────────────────────────────────────
    "2": {
        "ANT": "Antofagasta",    "CAL": "Calama",          "MAR": "María Elena",
        "MEJ": "Mejillones",     "OLL": "Ollagüe",         "SAN": "San Pedro de Atacama",
        "SIE": "Sierra Gorda",   "TAL": "Taltal",          "TOC": "Tocopilla",
    },
    # ── 3. Atacama ───────────────────────────────────────────────────
    "3": {
        "ALT": "Alto del Carmen","CAL": "Caldera",         "CHA": "Chañaral",
        "COP": "Copiapó",        "DIE": "Diego de Almagro","FRE": "Freirina",
        "HUA": "Huasco",         "TIE": "Tierra Amarilla", "VAL": "Vallenar",
    },
    # ── 4. Coquimbo ──────────────────────────────────────────────────
    "4": {
        "AND": "Andacollo",      "CAN": "Canela",          "COM": "Combarbalá",
        "COQ": "Coquimbo",       "ILL": "Illapel",         "LAH": "La Higuera",
        "LAS": "La Serena",      "LOS": "Los Vilos",       "MON": "Monte Patria",
        "OVA": "Ovalle",         "PAI": "Paihuano",        "PUN": "Punitaqui",
        "RIO": "Río Hurtado",    "SAL": "Salamanca",
        "VIC": "Vicuña",         "VIC¥": "Vicuña",          # ERP guarda "VIC" pero nombre "VICU¥A"
    },
    # ── 5. Valparaíso ────────────────────────────────────────────────
    "5": {
        "ALG": "Algarrobo",      "CAB": "Cabildo",         "CAL": "Calle Larga",
        "CAR": "Cartagena",      "CAS": "Casablanca",      "CAT": "Catemu",
        "CON": "Concón",         "ELQ": "El Quisco",       "ELT": "El Tabo",
        "HIJ": "Hijuelas",       "ISL": "Isla de Pascua",  "JUA": "Juan Fernández",
        "LAC": "La Calera",      "LAL": "La Ligua",        "LAR": "La Cruz",
        "LIM": "Limache",        "LLA": "Llay Llay",       "LOS": "Los Andes",
        "NOG": "Nogales",        "OLM": "Olmué",           "PAN": "Panquehue",
        "PAP": "Papudo",         "PET": "Petorca",         "PUC": "Puchuncaví",
        "PUT": "Putaendo",       "QUI": "Quilpué",         "QUL": "Quillota",
        "QUN": "Quintero",       "RIN": "Rinconada",       "SAE": "San Esteban",
        "SAF": "San Felipe",     "SAM": "Santa María",     "SAN": "San Antonio",
        "SAT": "Santo Domingo",  "VAL": "Valparaíso",      "VIL": "Villa Alemana",
        "VI¥": "Viña del Mar",   "ZAP": "Zapallar",
    },
    # ── 6. O'Higgins ─────────────────────────────────────────────────
    "6": {
        "CHE": "Chépica",        "CHI": "Chimbarongo",     "COD": "Codegua",
        "COI": "Coínco",         "COL": "Coltauco",        "DO¥": "Doñihue",
        "GRA": "Graneros",       "LAE": "La Estrella",     "LAS": "Las Cabras",
        "LIT": "Litueche",       "LOL": "Lolol",           "MAC": "Machalí",
        "MAL": "Malloa",         "MAR": "Marchihue",       "NAN": "Nancagua",
        "NAV": "Navidad",        "OLI": "Olivar",          "PAL": "Palmilla",
        "PAR": "Paredones",      "PER": "Peralillo",       "PEU": "Peumo",
        "PIC": "Pichidegua",     "PIH": "Pichilemu",       "PLA": "Placilla",
        "PUM": "Pumanque",       "QUI": "Quinta de Tilcoco","RAN": "Rancagua",
        "REN": "Rengo",          "REQ": "Requínoa",        "SAC": "Santa Cruz",
        "SAF": "San Fernando",   "SAN": "San Francisco de Mostazal",
        "SAV": "San Vicente",
    },
    # ── 7. Maule ─────────────────────────────────────────────────────
    "7": {
        "CAU": "Cauquenes",      "CHA": "Chanco",          "COL": "Colbún",
        "CON": "Constitución",   "CUE": "Curepto",         "CUR": "Curicó",
        "EMP": "Empedrado",      "HUA": "Hualañé",         "LIC": "Licantén",
        "LIN": "Linares",        "LON": "Longaví",         "MAU": "Maule",
        "MOL": "Molina",         "PAR": "Parral",          "PEL": "Pelarco",
        "PEN": "Pencahue",       "PEU": "Pelluhue",        "RAU": "Rauco",
        "RET": "Retiro",         "RIO": "Río Claro",       "ROM": "Romeral",
        "SAG": "Sagrada Familia","SAJ": "San Javier",       "SAN": "San Clemente",
        "SAR": "San Rafael",     "TAL": "Talca",           "TEN": "Teno",
        "VIC": "Vichuquén",      "VIL": "Villa Alegre",     "YER": "Yerbas Buenas",
    },
    # ── 8. Biobío ────────────────────────────────────────────────────
    "8": {
        "ANT": "Antuco",         "ARA": "Arauco",          "CAB": "Cabrero",
        "CA¥": "Cañete",         "CHG": "Chiguayante",     "CON": "Concepción",
        "COR": "Coronel",        "COT": "Contulmo",        "CUR": "Curanilahue",
        "FLO": "Florida",        "HLP": "Hualpén",         "HUA": "Hualqui",
        "LAJ": "Laja",           "LEB": "Lebu",            "LOA": "Los Ángeles",
        "LOS": "Los Álamos",     "LOT": "Lota",            "MUL": "Mulchén",
        "NAC": "Nacimiento",     "NEG": "Negrete",         "PEN": "Penco",
        "QUA": "Quilaco",        "QUE": "Quilleco",        "SAB": "Santa Bárbara",
        "SAG": "San Gabriel Ñiquén","SAJ": "Santa Juana",  "SAR": "San Rosendo",
        "SPZ": "San Pedro de la Paz","TAL": "Talcahuano",  "TIR": "Tirúa",
        "TOM": "Tomé",           "TUC": "Tucapel",         "YUM": "Yumbel",
    },
    # ── 9. Araucanía ─────────────────────────────────────────────────
    "9": {
        "ANG": "Angol",          "CAR": "Carahue",         "COL": "Collipulli",
        "CUA": "Curarrehue",     "CUN": "Cunco",           "CUR": "Curacautín",
        "ERC": "Ercilla",        "FRE": "Freire",          "GAL": "Galvarino",
        "GOR": "Gorbea",         "LAU": "Lautaro",         "LOC": "Loncoche",
        "LON": "Lonquimay",      "LOS": "Los Sauces",      "LUM": "Lumaco",
        "MEL": "Melipeuco",      "NUE": "Nueva Imperial",  "PER": "Perquenco",
        "PIT": "Pitrufquén",     "PUC": "Pucón",           "PUR": "Purén",
        "REN": "Renaico",        "SAA": "Saavedra",        "TEM": "Temuco",
        "TEO": "Teodoro Schmidt","TOL": "Toltén",          "TRA": "Traiguén",
        "VIA": "Villarrica",     "VIC": "Victoria",        "VIL": "Vilcún",
    },
    # ── 10. Los Lagos ────────────────────────────────────────────────
    "10": {
        "ANC": "Ancud",          "CAL": "Calbuco",         "CAS": "Castro",
        "CHA": "Chaitén",        "CHO": "Chonchi",         "COC": "Cochamó",
        "CUR": "Curaco de Vélez","DAL": "Dalcahue",         "FRE": "Fresia",
        "FRU": "Frutillar",      "FUA": "Futaleufú",        "HAU": "Hualaihué",
        "LLA": "Llanquihue",     "LOM": "Los Muermos",     "MAU": "Maullín",
        "OSO": "Osorno",         "PAL": "Palena",          "PUE": "Puerto Octay",
        "PUQ": "Puqueldón",      "PUR": "Purranque",       "PUT": "Puerto Montt",
        "PUV": "Puerto Varas",   "PUY": "Puyehue",         "QUE": "Queilén",
        "QUI": "Quinchao",       "QUL": "Quellón",         "QUM": "Quemchi",
        "RIN": "Río Negro",      "SAJ": "San Juan de la Costa","SAP": "San Pablo",
    },
    # ── 11. Aysén ────────────────────────────────────────────────────
    "11": {
        "AYS": "Aysén",          "CHI": "Chile Chico",     "CIS": "Cisnes",
        "COC": "Cochrane",       "COY": "Coyhaique",       "GUA": "Guaitecas",
        "LAG": "Lago Verde",     "OHI": "O'Higgins",       "RIO": "Río Ibáñez",
        "TOR": "Tortel",
    },
    # ── 12. Magallanes ───────────────────────────────────────────────
    "12": {
        "LAG": "Laguna Blanca",  "NAV": "Cabo de Hornos (Navarino)","POR": "Porvenir",
        "PRI": "Primavera",      "PUE": "Puerto Natales",  "PUN": "Punta Arenas",
        "RIO": "Río Verde",      "SAN": "San Gregorio",    "TIM": "Timaukel",
        "TOR": "Torres del Paine",
    },
    # ── 13. Región Metropolitana ─────────────────────────────────────
    "13": {
        "ALH": "Alhué",          "BUI": "Buin",            "CAL": "Calera de Tango",
        "CEI": "Cerrillos",      "CER": "Cerro Navia",     "COL": "Colina",
        "CON": "Conchalí",       "CUR": "Curacaví",        "ELB": "El Bosque",
        "ELM": "El Monte",       "EST": "Estación Central","HUE": "Huechuraba",
        "IND": "Independencia",  "ISL": "Isla de Maipo",   "LAC": "La Cisterna",
        "LAF": "La Florida",     "LAG": "La Granja",       "LAM": "Lampa",
        "LAP": "La Pintana",     "LAR": "La Reina",        "LAS": "Las Condes",
        "LOB": "Lo Barnechea",   "LOE": "Lo Espejo",       "LOP": "Lo Prado",
        "MAC": "Macul",          "MAI": "Maipú",           "MAR": "María Pinto",
        "MEL": "Melipilla",      "PAD": "Padre Hurtado",   "PAI": "Paine",
        "PEA": "Peñalolén",      "PED": "Pedro Aguirre Cerda",
        "PE¥": "Peñaflor",       "PIR": "Pirque",          "PRO": "Providencia",
        "PUD": "Pudahuel",       "PUE": "Puente Alto",     "QUI": "Quinta Normal",
        "QUL": "Quilicura",      "REC": "Recoleta",        "REN": "Renca",
        "SAB": "San Bernardo",   "SAJ": "San Joaquín",     "SAM": "San Miguel",
        "SAN": "Santiago",       "SAO": "San José de Maipo","SAP": "San Pedro",
        "SAR": "San Ramón",      "TAL": "Talagante",       "TIL": "Tiltil",
        "VIT": "Vitacura",       "¥U¥": "Ñuñoa",
    },
    # ── 14. Arica y Parinacota ───────────────────────────────────────
    "14": {
        "ARI": "Arica",          "CAM": "Camarones",       "GNL": "General Lagos",
        "PUT": "Putre",
    },
    # ── 15. Ñuble ────────────────────────────────────────────────────
    "15": {
        "BUL": "Bulnes",         "CHI": "Chillán",         "CHV": "Chillán Viejo",
        "COE": "Coelemu",        "COI": "Coihueco",        "CQC": "Cobquecura",
        "ELC": "El Carmen",      "NIN": "Ninhue",          "PEM": "Pemuco",
        "PIN": "Pinto",          "POR": "Portezuelo",      "QRH": "Quirihue",
        "QUI": "Quillón",        "RAN": "Ránquil",         "SNC": "San Carlos",
        "SNF": "San Fabián",     "SNI": "San Ignacio",     "SNN": "San Nicolás",
        "TRE": "Trehuaco",       "YNY": "Yungay",          "ÑIQ": "Ñiquén",
    },
    # ── 16. Los Ríos ─────────────────────────────────────────────────
    "16": {
        "COR": "Corral",         "FUT": "Futrono",         "LAG": "Los Lagos",
        "LAN": "Lanco",          "LAU": "La Unión",        "MAF": "Máfil",
        "MAQ": "Mariquina",      "PAI": "Paillaco",        "PAN": "Panguipulli",
        "RAN": "Lago Ranco",     "RBU": "Río Bueno",       "VAL": "Valdivia",
    },
}

REGION_NOMBRES: dict[str, str] = {
    "1": "Tarapacá",                "2": "Antofagasta",          "3": "Atacama",
    "4": "Coquimbo",                "5": "Valparaíso",
    "6": "Libertador Gral. Bernardo O'Higgins",
    "7": "Maule",                   "8": "Biobío",               "9": "La Araucanía",
    "10": "Los Lagos",              "11": "Aysén",               "12": "Magallanes",
    "13": "Metropolitana",          "14": "Arica y Parinacota",
    "15": "Ñuble",                  "16": "Los Ríos",
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


def fix_yen_to_n(text: str) -> str:
    """Convierte el carácter "¥" (yen, byte 0xA5) a "Ñ" — y "¥" minúscula
    a "ñ" cuando aparece en contexto minúsculo.

    El ERP Random guarda Ñ como "¥" por codificación CP437/CP850 antigua.
    Esta función se aplica al MOSTRAR datos al usuario; los códigos de
    búsqueda contra el ERP mantienen el "¥" literal porque así están
    guardados allá.

    Ejemplos:
      "VICU¥A"     → "VICUÑA"
      "DO¥IHUE"    → "DOÑIHUE"
      "VI¥A DEL MAR" → "VIÑA DEL MAR"
      "Vicu¥a"     → "Vicuña"
    """
    if not text:
        return ""
    s = str(text)
    # Reemplazo agresivo: cualquier ¥ → Ñ. Después, si todo el texto está
    # en minúsculas (excepto la Ñ recién introducida), convertir Ñ a ñ.
    result = s.replace("¥", "Ñ")
    # Casos típicos: "Vicu¥a" donde el resto está en minúsculas o title-case
    # Detección simple: si hay letras minúsculas y "Ñ" no está al inicio
    # de palabra, convertir a "ñ"
    if any(c.islower() for c in result) and result != result.upper():
        # title-case con Ñ minúscula
        out = []
        for i, c in enumerate(result):
            if c == "Ñ":
                # Si el carácter siguiente es minúscula, la Ñ también va minúscula
                if i + 1 < len(result) and result[i + 1].islower():
                    out.append("ñ")
                else:
                    out.append(c)
            else:
                out.append(c)
        result = "".join(out)
    return result


def cmen_to_comuna(cien: str, cmen: str) -> str:
    """Convierte (CIEN_región, CMEN_comuna) a nombre de comuna legible.

    Args:
        cien: código numérico de región (1-16). Acepta "1", "01", "001".
        cmen: código de 3-4 chars (puede contener "¥" para Ñ).

    Si no hay match, intenta `fix_yen_to_n(cmen)` como fallback final
    (sirve como seed para autocomplete).
    """
    if not cmen:
        return ""
    # Normalizar región: quitar ceros a la izquierda. "013" → "13", "01" → "1".
    cien_key = str(cien).lstrip("0") or "0"
    cmen_key = str(cmen).upper().strip()

    region_map = CMEN_MAP.get(cien_key, {})
    nombre = region_map.get(cmen_key)
    if nombre:
        return nombre
    # Fallback: buscar en todas las regiones (por si el CIEN viene mal)
    for rmap in CMEN_MAP.values():
        if cmen_key in rmap:
            return rmap[cmen_key]
    # Último recurso: devolver el código con ¥ convertido a Ñ
    return fix_yen_to_n(cmen_key)


def resolve_comuna(val: str, prefer_cien: str = "13") -> str:
    """Resuelve un valor que puede ser código (CEI, VI¥, 3-4 chars) o
    nombre (CERRILLOS, VICU¥A) a nombre legible.

    Args:
        val: el valor a resolver, sea código o nombre.
        prefer_cien: región preferida al buscar códigos ambiguos.
                     Default "13" (Metropolitana, lo más común para ILUS).
    """
    if not val:
        return ""
    s = str(val).strip()
    if not s:
        return ""

    # Si parece código (≤4 chars, mayúsculas o "¥", sin números)
    looks_like_code = (
        len(s) <= 4 and
        s.upper() == s and
        all(c.isalpha() or c == "¥" for c in s)
    )
    if looks_like_code:
        nombre = cmen_to_comuna(prefer_cien, s)
        if nombre and nombre != fix_yen_to_n(s):
            return nombre
        for cien in CMEN_MAP.keys():
            r = cmen_to_comuna(cien, s)
            if r and r != fix_yen_to_n(s):
                return r
    # Es nombre — convertir ¥ a Ñ y capitalizar si viene en mayúsculas
    s_normalized = fix_yen_to_n(s)
    return s_normalized.title() if s_normalized.upper() == s_normalized else s_normalized


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
        # Variantes principales
        "OBEN", "OBENEN", "OBDO", "OBSERVA", "OBSERVACIONES",
        "OBSCLI", "OBSDOC", "NOTAS", "COMENTARIO", "OBSERVACION",
        "REFERENCIA", "DETOFE",
        # ★★★ Textos libres del ERP Random (donde típicamente está
        # información como "QUIEBRE NVV-7623" — referencias a NV padre)
        "TEXTO1", "TEXTO2", "TEXTO3", "TEXTO4", "TEXTO5",
        "TXTODO", "TXDO", "NOTA", "NOTADO",
        "DESDO", "DETDO", "DESCDO", "DESCRIPCION",
        "GLOSADO", "GLOSA", "MEMODO",
    )
    # Claves en las LÍNEAS (maeddo) — el ERP Random embebe datos del cliente
    # en las primeras líneas cuando el header no los trae (típico en WEB/NVV)
    LINE_NAME_KEYS = (
        "NOKOEN", "NOKOENDE", "NOMENDE", "RAZSOCDE",
        "NRAZON", "NOMENT", "NOMBRE", "DESCDESP",
    )
    LINE_ADDR_KEYS = ("DIEN", "DIENDESP", "DIENDE", "DIRECCION", "DIRECDESP")
    LINE_COMUNA_KEYS = ("COMUNA", "CMEN", "NOKOCOMU", "NOKOZO")
    LINE_OBS_KEYS = (
        "OBDO", "OBSERVA", "OBSERVACION", "OBENEN", "OBLI",
        # ★★★ Textos libres por línea
        "TEXTO1", "TEXTO2", "TXLI", "NOTLI", "GLOSALI", "DETLI",
        "DESLI", "DESCLI", "OBSLI", "REFLI",
    )
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
        """Devuelve el primer valor no-vacío entre las keys dadas.

        Búsqueda 100% case-insensitive: construye un índice de las keys del
        dict en lowercase y compara cada key buscada contra ese índice.
        Esto evita fallar si el ERP devuelve "obdo", "Obdo" u "OBDO".
        """
        if not d:
            return ""
        # Construir índice {lowercase: valor_real} para acceso CI
        lower_index = {str(k).lower(): v for k, v in d.items()}
        for k in keys:
            v = lower_index.get(str(k).lower())
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    @classmethod
    def _scan_lines(cls, lines: list[dict]) -> dict:
        """Extrae datos del cliente embebidos en las líneas.

        Random ERP guarda NOKOEN/DIEN/COMUNA/OBDO en CADA LÍNEA de maeddo
        (no en el header maeedo). Típicamente todas las líneas tienen los
        mismos valores, pero a veces algunas líneas tienen OBDO con info
        relevante y otras vacío o con códigos.

        Estrategia:
          - Escanea TODAS las líneas (no se detiene en la primera).
          - Para cada campo, retiene el PRIMER valor no-vacío encontrado.
          - Para OBDO: recolecta TODOS los valores únicos no-vacíos y los
            concatena con " / " — así si una línea tiene "OC NRO. 47216"
            y otra "CAMBIO DE DCTO FCV-10201" se ven ambos.

        Aplica fix_yen_to_n (¥ → Ñ) a textos legibles.
        """
        out = {
            "nombre": "", "direccion": "", "comuna": "", "obs": "", "zona": "",
            "tipo_operacion": "", "tipo_codigo": "",
        }
        if not lines:
            return out
        obs_seen: list[str] = []  # observaciones únicas en orden
        zz_skus_found: list[tuple[str, str]] = []  # [(sku, descripcion)]
        for ln in lines:
            if not isinstance(ln, dict):
                continue
            if not out["nombre"]:
                out["nombre"] = fix_yen_to_n(cls._pick(ln, cls.LINE_NAME_KEYS)).title()
            if not out["direccion"]:
                out["direccion"] = fix_yen_to_n(cls._pick(ln, cls.LINE_ADDR_KEYS)).title()
            if not out["comuna"]:
                # Comuna: NO aplicar fix_yen_to_n al CÓDIGO crudo porque
                # "VI¥" debe llegar literal a resolve_comuna() (ahí se hace
                # la conversión cuando se resuelve a nombre).
                raw_c = cls._pick(ln, cls.LINE_COMUNA_KEYS)
                out["comuna"] = raw_c
            if not out["zona"]:
                out["zona"] = cls._pick(ln, cls.LINE_ZONA_KEYS)
            # OBSERVACIONES: recolectar TODAS las únicas
            obs_line = fix_yen_to_n(cls._pick(ln, cls.LINE_OBS_KEYS))
            if obs_line and obs_line not in obs_seen:
                # Ignorar valores que son obviamente IDs numéricos cortos
                # (ej. "4039", "#1582") — esos no son observaciones reales
                clean = obs_line.lstrip("#").strip()
                if not (clean.isdigit() and len(clean) <= 6):
                    obs_seen.append(obs_line)
            # TIPO DE OPERACIÓN: detectar SKU ZZ y guardar su descripción
            sku = (cls._pick(ln, ("KOPRCT", "SKU")) or "").upper()
            if sku.startswith("ZZ"):
                desc = cls._pick(ln, ("NOKOPR", "DESCRIPCION", "NOMBRE"))
                zz_skus_found.append((sku, desc))
        if obs_seen:
            out["obs"] = " / ".join(obs_seen[:3])  # max 3 distintas
        # ★★★ TIPO DE OPERACIÓN derivado de SKUs ZZ ★★★
        # Mapeo de prioridad (en orden de importancia para clasificar):
        #   ZZRETIRO       → "Retiro en Bodega"
        #   ZZINSTALACION  → "Instalación"
        #   ZZSERVTEC      → "Visita Técnica"
        #   ZZMANTENCION   → "Mantención"
        #   ZZINGREPUESTO  → "Repuestos"
        #   ZZINGARREQUIP  → "Ingreso/Arreglo Equipo"
        #   ZZBODEGA       → "Bodega"
        #   ZZENVIO        → "Despacho"
        op_priority = [
            ("ZZRETIRO",       "Retiro en Bodega"),
            ("ZZINSTALACION",  "Instalación"),
            ("ZZSERVTEC",      "Visita Técnica"),
            ("ZZMANTENCION",   "Mantención"),
            ("ZZINGREPUESTO",  "Venta de Repuestos"),
            ("ZZINGARREQUIP",  "Ingreso/Arreglo Equipo"),
            ("ZZBODEGA",       "Arriendo de Bodega"),
            ("ZZENVIO",        "Despacho de Productos"),
        ]
        zz_codes_found = [s for s, _ in zz_skus_found]
        for code, label in op_priority:
            if code in zz_codes_found:
                out["tipo_operacion"] = label
                out["tipo_codigo"] = code
                break
        # Si hay descripción específica del ERP (NOKOPR del SKU ZZ), úsala
        # — el ERP a veces tiene "Retiro en Bodega" como descripción exacta
        if out["tipo_codigo"]:
            for sku, desc in zz_skus_found:
                if sku == out["tipo_codigo"] and desc:
                    # Usar la descripción del ERP solo si es informativa
                    desc_clean = desc.strip()
                    if len(desc_clean) > 3:
                        out["tipo_operacion"] = desc_clean
                        break
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
        raw_obs_table: dict = {}   # ★ NUEVO: MAEEDOOB (tabla de observaciones)
        last_err: Optional[Exception] = None
        matched_nudo: Optional[str] = None

        for nv in nudos:
            diag["nudo_tried"].append(nv)
            try:
                body = self._get("/documentos/render",
                                 {"tido": erp_tido, "nudo": nv, "empresa": "01"})
                data = body.get("data") or []
                if data:
                    item = data[0]
                    raw_header = item.get("maeedo") or {}
                    raw_lines  = item.get("maeddo") or []
                    # ★★★ TABLA MAEEDOOB — observaciones del documento ★★★
                    # Según el diccionario Random, OBDO vive en tabla separada
                    # llamada MAEEDOOB (página 5 del diccionario). La API puede
                    # devolverla en varias ubicaciones posibles del JSON:
                    #   1. data[0].maeedoob  (sibling de maeedo/maeddo)
                    #   2. data[0].maeedoob[0]  (a veces es array)
                    #   3. data[0].maeedo.maeedoob  (anidado en header)
                    # Intentamos todas.
                    raw_obs_table = {}
                    cand = (
                        item.get("maeedoob")
                        or item.get("MAEEDOOB")
                        or raw_header.get("maeedoob")
                        or raw_header.get("MAEEDOOB")
                        or {}
                    )
                    if isinstance(cand, list) and cand:
                        # Si es array, tomar el primer registro
                        raw_obs_table = cand[0] if isinstance(cand[0], dict) else {}
                    elif isinstance(cand, dict):
                        raw_obs_table = cand

                    matched_nudo = nv
                    diag["match_nudo"] = nv
                    diag["fallback_chain"].append(f"render OK con nudo={nv}")
                    if raw_obs_table:
                        diag["fallback_chain"].append(
                            f"maeedoob encontrada ({len(raw_obs_table)} campos)"
                        )
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

        # ── Extraer OBSERVACIÓN DEL DOCUMENTO desde MAEEDOOB ──────────
        # ★★★ FUENTE PRIMARIA: tabla MAEEDOOB (página 5 del diccionario) ★★★
        # Campo OBDO = "Observacion". También se concatenan TEXTO1..TEXTO15
        # como información adicional si vienen llenos.
        obs_table_main = fix_yen_to_n(self._pick(raw_obs_table, ("OBDO",)))
        # Textos adicionales TEXTO1..TEXTO15
        obs_extra_texts: list[str] = []
        for n in range(1, 16):
            txt = self._pick(raw_obs_table, (f"TEXTO{n}",))
            if txt:
                txt_clean = fix_yen_to_n(txt).strip()
                if txt_clean and txt_clean not in obs_extra_texts:
                    obs_extra_texts.append(txt_clean)
        # Construir observación final desde MAEEDOOB
        obs_from_maeedoob = ""
        if obs_table_main:
            obs_from_maeedoob = obs_table_main
            if obs_extra_texts:
                obs_from_maeedoob += " / " + " / ".join(obs_extra_texts[:3])
        elif obs_extra_texts:
            obs_from_maeedoob = " / ".join(obs_extra_texts[:5])

        # DIENDESP (dirección de despacho) también vive en MAEEDOOB
        dir_despacho_obs = fix_yen_to_n(self._pick(raw_obs_table, ("DIENDESP",)))

        # ── Extraer datos del cliente desde el header (MAEEDO) ───────
        # IMPORTANTE: aplicar fix_yen_to_n a todos los textos legibles
        # porque el ERP guarda Ñ como ¥ (codificación CP437/CP850 antigua).
        endo = self._pick(raw_header, self.HDR_RUT_KEYS)
        header_nombre = fix_yen_to_n(self._pick(raw_header, self.HDR_NAME_KEYS)).title()
        header_email = self._pick(raw_header, self.HDR_EMAIL_KEYS)
        header_fono = self._pick(raw_header, self.HDR_PHONE_KEYS)
        header_dir = fix_yen_to_n(self._pick(raw_header, self.HDR_ADDR_KEYS)).title()
        header_obs = fix_yen_to_n(self._pick(raw_header, self.HDR_OBS_KEYS))

        # ── Extraer datos del cliente desde las LÍNEAS (fallback) ──
        line_data = self._scan_lines(raw_lines)
        if line_data["nombre"]:
            diag["fallback_chain"].append(f"nombre desde línea: {line_data['nombre'][:30]}")
        # Aplicar como fallback si el header no los trajo
        nombre_efectivo = header_nombre or line_data["nombre"]
        dir_efectivo = header_dir or line_data["direccion"]
        # ★★★ OBSERVACIÓN — orden de prioridad:
        # 1. MAEEDOOB.OBDO (+ TEXTO1..TEXTO15)  ← FUENTE OFICIAL del ERP
        # 2. Campos viejos del header (compat con respuestas raras)
        # 3. OBDO en líneas (extraído por _scan_lines)
        obs_efectivo = obs_from_maeedoob or header_obs or line_data["obs"]
        if obs_from_maeedoob:
            diag["fallback_chain"].append(f"obs desde MAEEDOOB: {obs_from_maeedoob[:40]}")

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
            # Normalizar ¥ → Ñ en todos los textos legibles del cliente
            cliente_nombre = fix_yen_to_n(self._pick(ent, ("NOKOEN", "NRAZON", "NOMBRE"))).title()
            cliente_rut = self._pick(ent, ("RTEN", "ENDO")) or endo
            cliente_email = self._pick(ent, ("EMAIL", "EMAILCOMER", "MAIL"))
            cliente_telefono = normalize_phone_cl(self._pick(ent, ("FOEN", "FAEN", "FONO", "CELULAR")))
            cliente_dir_base = fix_yen_to_n(self._pick(ent, ("DIEN", "DIRECCION", "DIRECEN"))).title()
            cliente_cien = self._pick(ent, ("CIEN", "REGION"))
            cliente_cmen = self._pick(ent, ("CMEN", "COMUNA"))
            cliente_obs = fix_yen_to_n(self._pick(ent, ("OBEN", "OBSERVACIONES")))
            cliente_comuna_nombre = cmen_to_comuna(cliente_cien, cliente_cmen)

        # ── Consolidar — la prioridad depende del campo ────────────────
        #
        # Para datos IDENTITARIOS del cliente (nombre, email, teléfono):
        #   /entidades > header > líneas
        #   El cliente es la fuente más confiable de su propio contacto.
        #
        # Para OBSERVACIONES del documento:
        #   header_obs > líneas (OBDO) > /entidades (OBEN)
        #   La observación del DOCUMENTO (lo que escribió el operador ESTA
        #   venta) gana sobre la observación HISTÓRICA del cliente. Si no
        #   hay obs del doc, usamos el OBEN del cliente como último recurso.
        #
        # Para DIRECCIÓN:
        #   DIENDESP del doc > línea > /entidades > header del cliente
        #   La dirección de DESPACHO del documento gana sobre la del cliente.
        # ─────────────────────────────────────────────────────────────────
        cliente_nombre = cliente_nombre or nombre_efectivo
        cliente_email = cliente_email or header_email
        cliente_telefono = cliente_telefono or normalize_phone_cl(header_fono)
        # Dirección — prioridad:
        # 1. DIENDESP en MAEEDOOB (dirección oficial de despacho del doc)
        # 2. DIENDESP en header (compat)
        # 3. Dirección efectiva de líneas o entidad
        cliente_dir_final = (
            dir_despacho_obs                                                 # MAEEDOOB.DIENDESP
            or fix_yen_to_n(self._pick(raw_header, ("DIENDESP", "DIENDE")))  # header
            or dir_efectivo
            or cliente_dir_base
        )
        # ★★★ OBSERVACIONES: prioridad invertida — el documento manda ★★★
        cliente_obs = obs_efectivo or cliente_obs

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
        # NUEVO: mostrar la primera línea con OBDO no-vacío, no solo la
        # primera línea. Útil para diagnosticar por qué una observación
        # no aparece — si OBDO existe en raw_line_sample pero no en
        # observaciones, hay un bug de mapeo; si no aparece, el ERP no
        # devuelve OBDO para ese documento.
        raw_line_sample = {}
        chosen_line = None
        for ln in raw_lines or []:
            if not isinstance(ln, dict):
                continue
            # Priorizar línea con OBDO no vacío
            obdo_val = (ln.get("OBDO") or ln.get("obdo") or "").strip()
            if obdo_val and not chosen_line:
                chosen_line = ln
            if not chosen_line:
                chosen_line = ln
        if chosen_line:
            for k, v in chosen_line.items():
                if v is None:
                    continue
                sv = str(v).strip()
                if sv and sv not in ("0", "0.0", "0.00", "False"):
                    raw_line_sample[k] = sv[:200]

        diag["latency_ms"] = int((time.time() - t0) * 1000)
        # Telemetría de qué se extrajo (para Railway logs y /api/erp/peek)
        diag["extracted"] = {
            "cliente_nombre": bool(cliente_nombre),
            "email": bool(cliente_email),
            "telefono": bool(cliente_telefono),
            "direccion": bool(cliente_dir_final),
            "comuna": bool(comuna_final),
            "observaciones": bool(cliente_obs),
            "obs_source": (
                "maeedoob.OBDO" if obs_from_maeedoob and obs_table_main else
                "maeedoob.TEXTOn" if obs_from_maeedoob else
                "header" if header_obs else
                "linea_obdo" if line_data.get("obs") else
                "entidad_oben" if cliente_obs else
                "ninguno"
            ),
            "maeedoob_present": bool(raw_obs_table),
            "maeedoob_obdo_value": obs_table_main[:120] if obs_table_main else "",
        }
        # Snapshot de MAEEDOOB para diagnóstico
        raw_obs_sample = {}
        for k, v in (raw_obs_table or {}).items():
            if v is None:
                continue
            sv = str(v).strip()
            if sv:
                raw_obs_sample[k] = sv[:200]

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
            # Tipo de operación derivado de SKUs ZZ
            "tipo_operacion":   line_data.get("tipo_operacion", ""),
            "tipo_codigo":      line_data.get("tipo_codigo", ""),
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
            "raw_obs_sample":   raw_obs_sample,   # ★ NUEVO: campos de MAEEDOOB
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

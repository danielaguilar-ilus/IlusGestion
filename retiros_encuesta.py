# -*- coding: utf-8 -*-
"""ILUS Fitness · Retiros — MÓDULO DE ENCUESTA DE SATISFACCIÓN (creado, NO lanzado).

Pedido de Daniel (2026-10-06): «Esta semana pediremos una encuesta, hagamos algo espectacular… pero aún no la
lancemos: solo creémosla». Ampliado el mismo día: módulo completo con su base de datos, preguntas EDITABLES y
versionadas, pantalla de edición, enlace visible en la ficha del retiro, y cumplimiento de la Ley 21.719.

ESTADO: la encuesta existe y el personal puede verla, editarla y probarla, pero NINGÚN cliente la recibe.
  · Este módulo NO envía nada: ni correo, ni WhatsApp, ni aviso. No importa ningún canal de salida (una prueba
    estática lo vigila). El enlace solo se MUESTRA al personal.
  · Mientras `RETIROS_ENCUESTA_ACTIVA` esté apagada (por defecto) la página pública responde 404 «Esta encuesta aún no
    está disponible» a cualquiera que no sea personal con sesión; el personal la ve en VISTA PREVIA (no se guarda nada).

RUTAS
  GET  /retiros/encuesta                          resultados internos (permiso «retiros»)
  GET  /retiros/encuesta/preguntas                editor de preguntas (admin / superadmin / ret_horarios)
  GET  /retiros/encuesta/preguntas/datos          JSON del editor
  POST /retiros/encuesta/preguntas/guardar        crear / editar una pregunta (versiona si ya tiene respuestas)
  POST /retiros/encuesta/preguntas/<id>/activa    activar / archivar
  POST /retiros/encuesta/preguntas/<id>/mover     subir / bajar
  GET  /retiros/encuesta/ficha/<rid>              JSON para el bloque «Encuesta de satisfacción» de la ficha del retiro
  GET  /retiros/encuesta/vista-previa             la encuesta como la verá el cliente (solo personal; no guarda)
  POST /retiros/encuesta/vista-previa             prueba el envío: valida y muestra el «gracias», sin guardar
  GET/POST /retiros/encuesta/<token>              página pública (solo si RETIROS_ENCUESTA_ACTIVA=1)

BASE DE DATOS (REGLA #18: tablas propias, una sentencia por CREATE, vía mysql_execute, creadas de forma PEREZOSA en
el primer uso —no al arrancar— y la guardia _ddl_ya_aplicado las salta sin pedir lock si ya existen):
  pickup_encuesta_preguntas     preguntas versionadas (una fila por versión; `vigente` marca la actual)
  pickup_encuesta_invitaciones  una por retiro (UNIQUE request_id), con vencimiento, envío, consentimiento y aviso
  pickup_encuesta_respuestas    una por pregunta respondida; apunta a la VERSIÓN exacta que vio el cliente

PRIVACIDAD (Ley 19.628 modificada por la 21.719): minimización (no se pide nombre, RUT, correo ni teléfono; la
respuesta se asocia al retiro por un enlace con token), aviso breve antes de responder, consentimiento explícito
(checkbox no premarcado) con fecha y versión del aviso guardadas, NO se guarda la IP, conservación limitada
(MESES_CONSERVACION) y función de anonimización `purgar_vencidas` (no se agenda sola).
"""
import hashlib
import hmac
import json
import os
import random
import re
import secrets
from datetime import datetime, timedelta, timezone

from flask import (abort, jsonify, make_response, redirect, render_template, request,
                   url_for)

# ════════════════════════════════════════════════════════════════════════
#  CONSTANTES
# ════════════════════════════════════════════════════════════════════════
RUTA_BASE = "/retiros/encuesta"
DIAS_VIGENCIA_ENLACE = 30          # el enlace de una invitación vence a los 30 días de creada
MESES_CONSERVACION = 24            # luego las respuestas se anonimizan (ver purgar_vencidas)
MAX_TEXTO = 1000                   # largo máximo de una respuesta de texto libre
MAX_PREGUNTAS_RECOMENDADO = 4      # más de esto baja la tasa de respuesta (aviso en el editor, no es un tope)

# Aviso de privacidad (versionado: se guarda con cada consentimiento)
AVISO_VERSION = "2026-10-06"
RESPONSABLE_RAZON = "Sport and Health Solutions SPA"        # razón social (documento con efecto legal: REGLA #0)
RESPONSABLE_RUT = "76.996.964-0"
CONTACTO_DERECHOS = "soportetec@sphs.cl"
AVISO_FINALIDAD = "mejorar el servicio de retiros en bodega de ILUS Fitness"

TIPOS = {"estrellas": "Estrellas (1 a 5)", "si_no": "Sí / No", "opcion": "Opción (elegir una)",
         "texto": "Texto libre"}
COLORES_OPCION = ("verde", "ambar", "rojo", "gris")
ETIQUETAS_ESTRELLAS = {1: "Mala", 2: "Regular", 3: "Buena", 4: "Muy buena", 5: "Excelente"}
EMOJIS_ESTRELLAS = {1: "😞", 2: "🙁", 3: "😐", 4: "🙂", 5: "🤩"}

# Umbrales del semáforo
ESCALA_VERDE, ESCALA_AMBAR = 4.3, 3.6        # promedio 1–5
PCT_VERDE, PCT_AMBAR = 90.0, 75.0            # % «bueno» (sí / opciones verdes)
MIN_RESPUESTAS_CONFIABLES = 10

# Preguntas SEMILLA. Criterio (clientes exigentes de equipamiento fitness; lo que más pesa es que el pedido esté
# preparado a tiempo cuando llegan): 3 preguntas operacionales + 1 comentario opcional ≈ 30 segundos en el celular.
PREGUNTAS_SEMILLA = [
    {"clave": "pedido_listo", "orden": 10, "tipo": "opcion", "obligatoria": 1,
     "texto": "¿Tu pedido estaba listo cuando llegaste?",
     "ayuda": "Lo más importante para nosotros: que no pierdas tiempo.",
     "opciones": [{"valor": "listo", "etiqueta": "Sí, estaba listo", "color": "verde"},
                  {"valor": "espera_corta", "etiqueta": "Tuve que esperar menos de 10 minutos", "color": "ambar"},
                  {"valor": "espera_larga", "etiqueta": "Esperé más de 10 minutos", "color": "rojo"}]},
    {"clave": "tiempo_total", "orden": 20, "tipo": "estrellas", "obligatoria": 1,
     "texto": "¿Cómo calificas el tiempo desde que pediste tu retiro hasta que te fuiste con tu pedido?",
     "ayuda": "Desde que agendaste hasta que saliste de bodega.", "opciones": []},
    {"clave": "atencion_bodega", "orden": 30, "tipo": "estrellas", "obligatoria": 1,
     "texto": "¿Cómo fue la atención del equipo en bodega?",
     "ayuda": "Trato, orden y ayuda al cargar tu pedido.", "opciones": []},
    {"clave": "comentario", "orden": 40, "tipo": "texto", "obligatoria": 0,
     "texto": "¿Qué podemos hacer mejor para tu próximo retiro?",
     "ayuda": "Opcional. Por favor no escribas datos personales (RUT, teléfono, correo ni dirección).",
     "opciones": []},
]

_SQL_PREGUNTAS = """
    CREATE TABLE IF NOT EXISTS pickup_encuesta_preguntas (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        clave        VARCHAR(40) NOT NULL COMMENT 'identidad estable de la pregunta a través de sus versiones',
        version      INT NOT NULL DEFAULT 1,
        texto        VARCHAR(300) NOT NULL,
        ayuda        VARCHAR(300) NULL,
        tipo         VARCHAR(12) NOT NULL COMMENT 'estrellas | si_no | opcion | texto',
        opciones     LONGTEXT NULL COMMENT 'JSON [{valor, etiqueta, color}] (solo tipo opcion)',
        obligatoria  TINYINT(1) NOT NULL DEFAULT 1,
        activa       TINYINT(1) NOT NULL DEFAULT 1,
        orden        INT NOT NULL DEFAULT 0,
        vigente      TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1 = versión actual de la clave; las demás se conservan para las respuestas viejas',
        creado_en    DATETIME NOT NULL,
        creado_por   VARCHAR(190) NULL,
        UNIQUE KEY uq_encpreg_clave_version (clave, version),
        KEY idx_encpreg_vigente (vigente, activa, orden)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
_SQL_INVITACIONES = """
    CREATE TABLE IF NOT EXISTS pickup_encuesta_invitaciones (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        request_id       INT NULL COMMENT 'retiro; NULL cuando la invitación fue anonimizada',
        token_hash       CHAR(64) NOT NULL COMMENT 'sha256 del token; el enlace utilizable no se guarda',
        creada_en        DATETIME NOT NULL,
        vence_en         DATETIME NOT NULL,
        enviada_en       DATETIME NULL COMMENT 'lo marca el futuro envío; hoy nada se envía',
        respondida_en    DATETIME NULL,
        consentimiento_en DATETIME NULL,
        version_aviso    VARCHAR(20) NULL COMMENT 'versión del aviso de privacidad aceptado',
        anonimizada_en   DATETIME NULL,
        UNIQUE KEY uq_encinv_request (request_id),
        UNIQUE KEY uq_encinv_token (token_hash),
        KEY idx_encinv_respondida (respondida_en)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
_SQL_RESPUESTAS = """
    CREATE TABLE IF NOT EXISTS pickup_encuesta_respuestas (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        invitacion_id INT NOT NULL,
        pregunta_id   INT NOT NULL COMMENT 'la VERSIÓN exacta de la pregunta que vio el cliente',
        valor_num     TINYINT NULL,
        valor_texto   TEXT NULL,
        creado_en     DATETIME NOT NULL,
        UNIQUE KEY uq_encresp (invitacion_id, pregunta_id),
        KEY idx_encresp_pregunta (pregunta_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_VERDADERO = {"1", "true", "yes", "on", "si", "sí"}


def encuesta_activa():
    """Interruptor de lanzamiento. Apagado por defecto: nadie fuera del personal puede abrir la encuesta."""
    return (os.environ.get("RETIROS_ENCUESTA_ACTIVA") or "").strip().lower() in _VERDADERO


def _ahora():
    """Ahora, UTC sin zona (como guarda MySQL; REGLA #6). Los tests lo reemplazan."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _a_dt(v):
    if v is None or isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace(" ", "T"))
    except ValueError:
        return None


# ════════════════════════════════════════════════════════════════════════
#  TOKEN (HMAC derivado; NO es el public_token del seguimiento)
# ════════════════════════════════════════════════════════════════════════
def _base36(n):
    d = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "0"
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = d[r] + s
    return s


def generar_token(secreto, request_id, public_token):
    """`<id en base36>-<24 hex de HMAC-SHA256>`. El HMAC ata el enlace al retiro (id + public_token) con una clave del
    servidor: no se puede fabricar, no se puede adivinar el de otro retiro y no revela nada del cliente. NO es el
    public_token: si el enlace de la encuesta se filtra no abre el seguimiento (y viceversa). Se puede volver a
    mostrar al personal sin guardarlo. Cambiar el public_token del retiro invalida el enlace."""
    if not secreto or not public_token:
        return ""
    msg = f"encuesta:v1:{int(request_id)}:{public_token}".encode("utf-8")
    mac = hmac.new(str(secreto).encode("utf-8"), msg, hashlib.sha256).hexdigest()[:24]
    return f"{_base36(int(request_id))}-{mac}"


def _id_de_token(token):
    m = re.fullmatch(r"([0-9a-z]{1,10})-([0-9a-f]{24})", token or "")
    if not m:
        return None
    try:
        return int(m.group(1), 36)
    except ValueError:
        return None


def _hash_token(token):
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


# ════════════════════════════════════════════════════════════════════════
#  PREGUNTAS: normalización y validación de la definición (editor)
# ════════════════════════════════════════════════════════════════════════
def _texto_limpio(valor, maximo):
    if valor is None:
        return ""
    txt = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", str(valor)).replace("\r\n", "\n").strip()
    return txt[:maximo]


def _entero(valor, minimo, maximo):
    if valor is None:
        return None
    txt = str(valor).strip()
    if not re.fullmatch(r"\d{1,2}", txt):
        return None
    n = int(txt)
    return n if minimo <= n <= maximo else None


def validar_definicion(d):
    """Valida lo que manda el editor. Devuelve (limpia, errores)."""
    errores = {}
    texto = _texto_limpio(d.get("texto"), 300)
    if len(texto) < 8:
        errores["texto"] = "Escribe la pregunta completa (mínimo 8 letras)."
    ayuda = _texto_limpio(d.get("ayuda"), 300)
    tipo = str(d.get("tipo") or "").strip()
    if tipo not in TIPOS:
        errores["tipo"] = "Elige el tipo de pregunta."
    obligatoria = 1 if d.get("obligatoria") in (True, 1, "1", "true", "on", "si") else 0
    opciones = []
    if tipo == "opcion":
        crudas = d.get("opciones") or []
        usados = set()
        for i, o in enumerate(crudas if isinstance(crudas, list) else []):
            etiqueta = _texto_limpio((o or {}).get("etiqueta"), 120)
            if not etiqueta:
                continue
            valor = str((o or {}).get("valor") or "").strip()
            if not re.fullmatch(r"[a-z0-9_]{1,30}", valor) or valor in usados:
                n = i + 1
                while f"o{n}" in usados:
                    n += 1
                valor = f"o{n}"
            usados.add(valor)
            color = (o or {}).get("color") if (o or {}).get("color") in COLORES_OPCION else "gris"
            opciones.append({"valor": valor, "etiqueta": etiqueta, "color": color})
        if not 2 <= len(opciones) <= 6:
            errores["opciones"] = "Una pregunta de opción necesita entre 2 y 6 opciones con texto."
    return {"texto": texto, "ayuda": ayuda, "tipo": tipo, "obligatoria": obligatoria, "opciones": opciones}, errores


def _fila_a_pregunta(f):
    try:
        ops = json.loads(f.get("opciones") or "[]")
    except ValueError:
        ops = []
    return {"id": int(f["id"]), "clave": f["clave"], "version": int(f["version"]), "texto": f["texto"],
            "ayuda": f.get("ayuda") or "", "tipo": f["tipo"], "opciones": ops,
            "obligatoria": bool(f["obligatoria"]), "activa": bool(f["activa"]), "orden": int(f["orden"]),
            "vigente": bool(f["vigente"])}


def validar_respuestas(preguntas, datos):
    """Valida el envío contra las preguntas ACTIVAS vistas por el cliente.
    Devuelve (respuestas, errores). respuestas: [{pregunta_id, valor_num, valor_texto}]; errores: {clave: mensaje}.
    El consentimiento es obligatorio (checkbox no premarcado)."""
    respuestas, errores = [], {}
    for p in preguntas:
        campo = f"p_{p['id']}"
        crudo = datos.get(campo)
        vacio = crudo is None or str(crudo).strip() == ""
        if vacio:
            if p["obligatoria"]:
                errores[campo] = ("Cuéntanos tu opinión." if p["tipo"] == "texto" else "Elige una opción.")
            continue
        r = {"pregunta_id": p["id"], "valor_num": None, "valor_texto": None}
        if p["tipo"] == "estrellas":
            n = _entero(crudo, 1, 5)
            if n is None:
                errores[campo] = "Elige una opción de 1 a 5."
                continue
            r["valor_num"] = n
        elif p["tipo"] == "si_no":
            v = str(crudo).strip().lower()
            if v not in ("si", "sí", "1", "no", "0"):
                errores[campo] = "Elige sí o no."
                continue
            r["valor_num"] = 1 if v in ("si", "sí", "1") else 0
        elif p["tipo"] == "opcion":
            valor = str(crudo).strip()
            if valor not in {o["valor"] for o in p["opciones"]}:
                errores[campo] = "Elige una de las opciones."
                continue
            r["valor_texto"] = valor
        else:
            txt = _texto_limpio(crudo, MAX_TEXTO)
            if not txt:
                if p["obligatoria"]:
                    errores[campo] = "Cuéntanos tu opinión."
                continue
            r["valor_texto"] = txt
        respuestas.append(r)
    if str(datos.get("consentimiento") or "").strip().lower() not in _VERDADERO:
        errores["consentimiento"] = "Para enviar tus respuestas necesitamos que aceptes el aviso de privacidad."
    return respuestas, errores


# ════════════════════════════════════════════════════════════════════════
#  RESULTADOS: agregación + semáforo
# ════════════════════════════════════════════════════════════════════════
def _sem_escala(prom):
    if prom is None:
        return "gris"
    return "verde" if prom >= ESCALA_VERDE else "ambar" if prom >= ESCALA_AMBAR else "rojo"


def _sem_pct(pct):
    if pct is None:
        return "gris"
    return "verde" if pct >= PCT_VERDE else "ambar" if pct >= PCT_AMBAR else "rojo"


def armar_resultados(preguntas, filas):
    """preguntas: preguntas vigentes (dicts, cualquier estado). filas: agregados [{clave, valor_num, valor_texto, c}].
    Devuelve una tarjeta por pregunta NO de texto, con su KPI y semáforo. Las versiones de una misma pregunta
    (misma clave) se agrupan: el texto mostrado es el de la versión vigente."""
    por_clave = {}
    for f in filas:
        por_clave.setdefault(f["clave"], []).append(f)
    tarjetas = []
    for p in sorted(preguntas, key=lambda x: (x["orden"], x["id"])):
        if p["tipo"] == "texto":
            continue
        fs = por_clave.get(p["clave"], [])
        n = sum(int(f["c"]) for f in fs)
        t = {"clave": p["clave"], "texto": p["texto"], "tipo": p["tipo"], "activa": p["activa"], "n": n,
             "version": p["version"]}
        if p["tipo"] == "estrellas":
            dist = {i: 0 for i in range(1, 6)}
            suma = 0
            for f in fs:
                dist[int(f["valor_num"])] += int(f["c"])
                suma += int(f["valor_num"]) * int(f["c"])
            prom = round(suma / n, 2) if n else None
            t.update(prom=prom, color=_sem_escala(prom), kpi=(f"{prom:.1f}".replace(".", ",") if prom is not None else "–"),
                     kpi_sub="promedio de 1 a 5",
                     barras=[{"etiqueta": f"{i}", "c": dist[i], "pct": round(100.0 * dist[i] / n) if n else 0,
                              "color": "rojo" if i <= 2 else "ambar" if i == 3 else "verde"} for i in range(5, 0, -1)])
        elif p["tipo"] == "si_no":
            si = sum(int(f["c"]) for f in fs if int(f["valor_num"]) == 1)
            pct = round(100.0 * si / n, 1) if n else None
            t.update(pct=pct, color=_sem_pct(pct), kpi=(f"{pct}%".replace(".", ",") if pct is not None else "–"),
                     kpi_sub="respondió que sí",
                     barras=[{"etiqueta": "Sí", "c": si, "pct": round(100.0 * si / n) if n else 0, "color": "verde"},
                             {"etiqueta": "No", "c": n - si, "pct": round(100.0 * (n - si) / n) if n else 0,
                              "color": "rojo"}])
        else:   # opcion
            cuenta = {}
            for f in fs:
                cuenta[f["valor_texto"]] = cuenta.get(f["valor_texto"], 0) + int(f["c"])
            barras, buenas = [], 0
            for o in p["opciones"]:
                c = cuenta.pop(o["valor"], 0)
                barras.append({"etiqueta": o["etiqueta"], "c": c, "pct": round(100.0 * c / n) if n else 0,
                               "color": o["color"]})
                if o["color"] == "verde":
                    buenas += c
            for k, c in cuenta.items():     # opciones de versiones anteriores que ya no existen
                barras.append({"etiqueta": f"{k} (versión anterior)", "c": c, "pct": round(100.0 * c / n) if n else 0,
                               "color": "gris"})
            pct = round(100.0 * buenas / n, 1) if n else None
            t.update(pct=pct, color=_sem_pct(pct), kpi=(f"{pct}%".replace(".", ",") if pct is not None else "–"),
                     kpi_sub="eligió la mejor opción", barras=barras)
        tarjetas.append(t)
    return tarjetas


_COMENTARIOS_EJEMPLO = [
    "Todo muy ordenado. Llegué a la hora y en cinco minutos ya estaba cargando mi pedido.",
    "El equipo de bodega fue muy amable, me ayudaron a subir las cajas al auto.",
    "Me demoré un poco más de lo esperado en la fila, pero la atención fue excelente.",
    "Los correos son claros, siempre supe en qué estado iba mi retiro.",
    "Faltaba un accesorio de mi pedido. Lo resolvieron rápido, pero me hubiera gustado saberlo antes.",
    "Muy buena experiencia, agendar por internet es mucho más cómodo que llamar.",
    "Excelente servicio, lo recomendaría sin dudar.",
    "Podrían avisar por mensaje cuando el pedido esté listo para retirar.",
    "Rápido y sin papeleo. Así da gusto retirar.",
    "El estacionamiento estaba lleno, pero el resto perfecto.",
]


def datos_de_ejemplo(preguntas, n=64, semilla=2026):
    """Respuestas SINTÉTICAS (según las preguntas actuales) para que el equipo vea cómo se verán los resultados.
    No son clientes reales y NUNCA se guardan: se generan en memoria con semilla fija. → (filas, comentarios)"""
    rnd = random.Random(semilla)
    cuenta = {}
    comentarios = []
    ahora = _ahora()
    for i in range(n):
        for p in preguntas:
            if p["tipo"] == "estrellas":
                v = max(1, min(5, round(rnd.gauss(4.4 if p["clave"] != "tiempo_total" else 4.0, 0.9))))
                cuenta[(p["clave"], v, None)] = cuenta.get((p["clave"], v, None), 0) + 1
            elif p["tipo"] == "si_no":
                v = 1 if rnd.random() < 0.93 else 0
                cuenta[(p["clave"], v, None)] = cuenta.get((p["clave"], v, None), 0) + 1
            elif p["tipo"] == "opcion" and p["opciones"]:
                pesos = [70, 20, 10] + [5] * 3
                o = rnd.choices(p["opciones"], weights=pesos[:len(p["opciones"])])[0]["valor"]
                cuenta[(p["clave"], None, o)] = cuenta.get((p["clave"], None, o), 0) + 1
            elif p["tipo"] == "texto" and rnd.random() < 0.45:
                comentarios.append({"code": f"EJEMPLO-{i + 1:03d}", "pregunta": p["texto"],
                                    "texto": rnd.choice(_COMENTARIOS_EJEMPLO),
                                    "creado_en": ahora - timedelta(hours=rnd.randint(1, 20 * 24))})
    filas = [{"clave": k[0], "valor_num": k[1], "valor_texto": k[2], "c": c} for k, c in cuenta.items()]
    comentarios.sort(key=lambda c: c["creado_en"], reverse=True)
    return filas, comentarios, n


# ════════════════════════════════════════════════════════════════════════
#  CONSERVACIÓN: anonimización de lo vencido (NO se agenda sola)
# ════════════════════════════════════════════════════════════════════════
def purgar_vencidas(mysql_execute, mysql_fetchall, ahora=None, meses=MESES_CONSERVACION, dry_run=True):
    """Anonimiza lo que ya pasó el plazo de conservación (por defecto MESES_CONSERVACION = 24 meses):
      · borra el texto libre (puede traer datos personales que el cliente escribió),
      · desvincula la invitación del retiro (request_id = NULL) e invalida su enlace.
    Quedan solo los números y opciones, que ya no se pueden atribuir a un cliente (estadística anónima).
    No borra filas. `dry_run=True` (por defecto) solo cuenta. Devuelve {'candidatas', 'anonimizadas'}."""
    ahora = ahora or _ahora()
    limite = ahora - timedelta(days=round(meses * 30.4375))
    filas = mysql_fetchall(
        "SELECT id FROM pickup_encuesta_invitaciones WHERE anonimizada_en IS NULL "
        "AND COALESCE(respondida_en, creada_en) < %s ORDER BY id", (limite,)) or []
    if dry_run:
        return {"candidatas": len(filas), "anonimizadas": 0}
    for f in filas:
        mysql_execute(
            "UPDATE pickup_encuesta_respuestas SET valor_texto=NULL WHERE invitacion_id=%s AND pregunta_id IN "
            "(SELECT id FROM pickup_encuesta_preguntas WHERE tipo='texto')", (f["id"],))
        mysql_execute(
            "UPDATE pickup_encuesta_invitaciones SET request_id=NULL, token_hash=%s, anonimizada_en=%s WHERE id=%s",
            (hashlib.sha256(secrets.token_bytes(32)).hexdigest(), ahora, f["id"]))
    return {"candidatas": len(filas), "anonimizadas": len(filas)}


# ════════════════════════════════════════════════════════════════════════
#  REGISTRO DE RUTAS
# ════════════════════════════════════════════════════════════════════════
def register_encuesta_routes(app, ctx):
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    require_permission = ctx["require_permission"]
    g = ctx["g"]
    REQ = ctx.get("PICKUP_REQUESTS_TABLE") or "pickup_requests"
    _con_rowcount = ctx.get("mysql_execute_returning_rowcount")
    _fmt_chile_app = ctx.get("chile_fmt_filter")
    _base_publica = ctx.get("_public_base_url")
    _ESTADO = {"lista": False}

    # ── utilidades ───────────────────────────────────────────────────────
    def _secreto():
        return os.environ.get("ILUS_ENCUESTA_SECRET") or app.secret_key or ""

    def _perms():
        return getattr(g, "permissions", None) or {}

    def _es_personal():
        u = getattr(g, "user", None)
        return bool(u and (_perms().get("superadmin") or _perms().get("retiros")))

    def _puede_editar():
        """Editar las preguntas: admin, superadmin o la jefatura de Retiros (ret_horarios)."""
        u = getattr(g, "user", None)
        p = _perms()
        return bool(u and (p.get("superadmin") or p.get("admin") or p.get("ret_horarios")))

    def _quien():
        u = getattr(g, "user", None) or {}
        return str(u.get("nombre") or u.get("username") or u.get("id") or "")[:190]

    def _ejecutar(sql, params=()):
        if _con_rowcount:
            return int(_con_rowcount(sql, params) or 0)
        return int(mysql_execute(sql, params) or 0)

    def _asegurar_esquema():
        """Creación PEREZOSA (primer uso), no al arrancar; una sentencia por tabla (REGLA #18) + semilla idempotente."""
        if _ESTADO["lista"]:
            return
        mysql_execute(_SQL_PREGUNTAS)
        mysql_execute(_SQL_INVITACIONES)
        mysql_execute(_SQL_RESPUESTAS)
        hay = mysql_fetchone("SELECT COUNT(*) AS c FROM pickup_encuesta_preguntas") or {}
        if not int(hay.get("c") or 0):
            for s in PREGUNTAS_SEMILLA:
                _ejecutar(
                    "INSERT IGNORE INTO pickup_encuesta_preguntas (clave, version, texto, ayuda, tipo, opciones, "
                    "obligatoria, activa, orden, vigente, creado_en, creado_por) "
                    "VALUES (%s,1,%s,%s,%s,%s,%s,1,%s,1,%s,'semilla')",
                    (s["clave"], s["texto"], s["ayuda"], s["tipo"],
                     json.dumps(s["opciones"], ensure_ascii=False) if s["opciones"] else None,
                     s["obligatoria"], s["orden"], _ahora()))
        _ESTADO["lista"] = True

    def _reclamar(inv_id, ahora):
        """True si ESTA petición fue la que marcó la invitación como respondida (única ganadora)."""
        sql = ("UPDATE pickup_encuesta_invitaciones SET respondida_en=%s, consentimiento_en=%s, version_aviso=%s "
               "WHERE id=%s AND respondida_en IS NULL")
        params = (ahora, ahora, AVISO_VERSION, inv_id)
        if _con_rowcount:
            return int(_con_rowcount(sql, params) or 0) == 1
        mysql_execute(sql, params)
        fila = mysql_fetchone("SELECT respondida_en FROM pickup_encuesta_invitaciones WHERE id=%s", (inv_id,)) or {}
        return _a_dt(fila.get("respondida_en")) == ahora

    def _fmt_fecha(valor):
        dt = _a_dt(valor)
        if not dt:
            return ""
        if _fmt_chile_app:
            try:
                return _fmt_chile_app(dt)
            except Exception:
                pass
        try:
            from zoneinfo import ZoneInfo
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(ZoneInfo("America/Santiago")).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return str(dt)

    def _con_cabeceras(resp):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    def _pagina(plantilla, estado=200, **kw):
        kw.setdefault("etiquetas_estrellas", ETIQUETAS_ESTRELLAS)
        kw.setdefault("emojis", EMOJIS_ESTRELLAS)
        kw.setdefault("max_texto", MAX_TEXTO)
        kw.setdefault("errores", {})
        kw.setdefault("datos_form", {})
        kw.setdefault("aviso", {"version": AVISO_VERSION, "razon": RESPONSABLE_RAZON, "rut": RESPONSABLE_RUT,
                                "contacto": CONTACTO_DERECHOS, "finalidad": AVISO_FINALIDAD,
                                "meses": MESES_CONSERVACION})
        return _con_cabeceras(make_response(render_template(plantilla, **kw), estado))

    def _no_disponible():
        return _pagina("retiros/encuesta_no_disponible.html", 404)

    def _json(datos, estado=200):
        r = jsonify(datos)
        r.status_code = estado
        r.headers["Cache-Control"] = "no-store"
        return r

    # ── preguntas ────────────────────────────────────────────────────────
    def _preguntas_vigentes(solo_activas=False):
        _asegurar_esquema()
        sql = "SELECT * FROM pickup_encuesta_preguntas WHERE vigente=1"
        if solo_activas:
            sql += " AND activa=1"
        sql += " ORDER BY orden, id"
        return [_fila_a_pregunta(f) for f in (mysql_fetchall(sql) or [])]

    # ── invitaciones ─────────────────────────────────────────────────────
    def _retiro_de_token(token):
        rid = _id_de_token(token)
        if rid is None:
            return None
        fila = mysql_fetchone(f"SELECT id, status, public_token, closed_at FROM `{REQ}` WHERE id=%s LIMIT 1", (rid,))
        if not fila or not fila.get("public_token"):
            return None
        esperado = generar_token(_secreto(), fila["id"], fila["public_token"])
        if not esperado or not hmac.compare_digest(esperado, token):
            return None
        return fila

    def _invitacion_de(request_id):
        return mysql_fetchone("SELECT * FROM pickup_encuesta_invitaciones WHERE request_id=%s LIMIT 1",
                              (request_id,))

    def _obtener_o_crear_invitacion(request_id, token, cerrado_en=None):
        """Una por retiro (UNIQUE request_id; INSERT IGNORE: dos pestañas no la duplican). Vence a los
        DIAS_VIGENCIA_ENLACE días de creada. Devuelve la fila, o None si el token ya no corresponde a la guardada.
        Si la invitación fue anonimizada (conservación vencida) NO se vuelve a crear: el retiro ya cerró hace más
        que el plazo de conservación."""
        _asegurar_esquema()
        inv = _invitacion_de(request_id)
        if not inv:
            ahora = _ahora()
            cerrado = _a_dt(cerrado_en)
            if cerrado and cerrado < ahora - timedelta(days=round(MESES_CONSERVACION * 30.4375)):
                return None
            _ejecutar("INSERT IGNORE INTO pickup_encuesta_invitaciones (request_id, token_hash, creada_en, vence_en) "
                      "VALUES (%s,%s,%s,%s)",
                      (request_id, _hash_token(token), ahora, ahora + timedelta(days=DIAS_VIGENCIA_ENLACE)))
            inv = _invitacion_de(request_id)
        if not inv or inv.get("token_hash") != _hash_token(token):
            return None
        return inv

    def invitar(request_id):
        """ENGANCHE PARA EL LANZAMIENTO (apagado): devuelve {'ruta','token','vence'} para que el envío futuro lo
        agregue al correo «retiro completado». Con la encuesta apagada, o si el retiro aún no se retiró, devuelve None
        y NO escribe nada. No envía nada por sí mismo. Después de enviar, el que envía llama marcar_enviada()."""
        if not encuesta_activa():
            return None
        fila = mysql_fetchone(f"SELECT id, status, public_token, closed_at FROM `{REQ}` WHERE id=%s LIMIT 1", (request_id,))
        if not fila or fila.get("status") not in ("retirada", "cerrada"):
            return None
        token = generar_token(_secreto(), fila["id"], fila["public_token"])
        inv = _obtener_o_crear_invitacion(fila["id"], token, fila.get("closed_at")) if token else None
        if not inv:
            return None
        return {"ruta": f"{RUTA_BASE}/{token}", "token": token, "vence": _a_dt(inv.get("vence_en"))}

    def marcar_enviada(request_id):
        return _ejecutar("UPDATE pickup_encuesta_invitaciones SET enviada_en=%s WHERE request_id=%s "
                         "AND enviada_en IS NULL", (_ahora(), request_id))

    def token_para(request_id, public_token):
        return generar_token(_secreto(), request_id, public_token)

    def ruta_para(request_id, public_token):
        t = token_para(request_id, public_token)
        return f"{RUTA_BASE}/{t}" if t else ""

    app.extensions["retiros_encuesta"] = {
        "activa": encuesta_activa, "token_para": token_para, "ruta_para": ruta_para, "invitar": invitar,
        "marcar_enviada": marcar_enviada,
        "purgar": lambda dry_run=True, ahora=None: purgar_vencidas(mysql_execute, mysql_fetchall, ahora=ahora,
                                                                   dry_run=dry_run)}

    # ── VISTA PREVIA (solo personal; no guarda nada) ────────────────────
    @app.route(f"{RUTA_BASE}/vista-previa", methods=["GET", "POST"])
    @require_permission("retiros")
    def retiros_encuesta_vista_previa():
        preguntas = _preguntas_vigentes(solo_activas=True)
        accion = url_for("retiros_encuesta_vista_previa")
        if request.method == "POST":
            _, errores = validar_respuestas(preguntas, request.form)
            if errores:
                return _pagina("retiros/encuesta_publica.html", 422, vista_previa=True, preguntas=preguntas,
                               errores=errores, accion=accion, datos_form=request.form)
            return _pagina("retiros/encuesta_gracias.html", vista_previa=True)
        return _pagina("retiros/encuesta_publica.html", vista_previa=True, preguntas=preguntas, accion=accion)

    # ── RESULTADOS INTERNOS ─────────────────────────────────────────────
    @app.route(RUTA_BASE, methods=["GET"])
    @require_permission("retiros")
    def retiros_encuesta_resultados():
        demo = request.args.get("demo") == "1"
        try:
            por_pagina = int(request.args.get("por_pagina") or 25)
        except ValueError:
            por_pagina = 25
        if por_pagina not in (10, 25, 50, 100):
            por_pagina = 25
        try:
            pagina = max(1, int(request.args.get("pagina") or 1))
        except ValueError:
            pagina = 1
        error_datos = False
        preguntas, tarjetas, comentarios, total_coment = [], [], [], 0
        n_resp, n_inv, elegibles = 0, 0, None
        try:
            preguntas = _preguntas_vigentes()
            if demo:
                filas, ej_coment, n_resp = datos_de_ejemplo(preguntas)
                n_inv = n_resp
                total_coment = len(ej_coment)
                paginas = max(1, -(-total_coment // por_pagina))
                pagina = min(pagina, paginas)
                comentarios = ej_coment[(pagina - 1) * por_pagina: pagina * por_pagina]
            else:
                tot = mysql_fetchone(
                    "SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN respondida_en IS NOT NULL THEN 1 ELSE 0 END),0) "
                    "AS resp FROM pickup_encuesta_invitaciones") or {}
                n_inv, n_resp = int(tot.get("total") or 0), int(tot.get("resp") or 0)
                filas = mysql_fetchall(
                    "SELECT p.clave AS clave, r.valor_num AS valor_num, r.valor_texto AS valor_texto, COUNT(*) AS c "
                    "FROM pickup_encuesta_respuestas r JOIN pickup_encuesta_preguntas p ON p.id=r.pregunta_id "
                    "WHERE p.tipo<>'texto' GROUP BY p.clave, r.valor_num, r.valor_texto") or []
                cuenta = mysql_fetchone(
                    "SELECT COUNT(*) AS c FROM pickup_encuesta_respuestas r JOIN pickup_encuesta_preguntas p "
                    "ON p.id=r.pregunta_id WHERE p.tipo='texto' AND COALESCE(r.valor_texto,'')<>''") or {}
                total_coment = int(cuenta.get("c") or 0)
                paginas = max(1, -(-total_coment // por_pagina))
                pagina = min(pagina, paginas)
                comentarios = mysql_fetchall(
                    f"SELECT r.valor_texto AS texto, r.creado_en AS creado_en, p.texto AS pregunta, q.code AS code "
                    f"FROM pickup_encuesta_respuestas r JOIN pickup_encuesta_preguntas p ON p.id=r.pregunta_id "
                    f"JOIN pickup_encuesta_invitaciones i ON i.id=r.invitacion_id "
                    f"LEFT JOIN `{REQ}` q ON q.id=i.request_id "
                    f"WHERE p.tipo='texto' AND COALESCE(r.valor_texto,'')<>'' "
                    f"ORDER BY r.creado_en DESC, r.id DESC LIMIT %s OFFSET %s",
                    (por_pagina, (pagina - 1) * por_pagina)) or []
                try:
                    fila = mysql_fetchone(
                        f"SELECT COUNT(*) AS c FROM `{REQ}` WHERE status IN ('retirada','cerrada') "
                        f"AND closed_at IS NOT NULL AND closed_at >= %s", (_ahora() - timedelta(days=30),))
                    elegibles = int((fila or {}).get("c") or 0)
                except Exception:
                    elegibles = None
            tarjetas = armar_resultados(preguntas, filas)
        except Exception as e:
            print(f"[ILUS][ENCUESTA] resultados: {type(e).__name__}", flush=True)
            error_datos = True
        paginas = max(1, -(-total_coment // por_pagina))
        desde = (pagina - 1) * por_pagina + 1 if total_coment else 0
        hasta = min(pagina * por_pagina, total_coment)
        lista = [{"code": c.get("code") or "", "pregunta": c.get("pregunta") or "", "texto": c.get("texto") or "",
                  "cuando": _fmt_fecha(c.get("creado_en"))} for c in comentarios]
        return _pagina("retiros/encuesta_resultados.html", tarjetas=tarjetas, comentarios=lista, demo=demo,
                       activa=encuesta_activa(), error_datos=error_datos, elegibles=elegibles, n_resp=n_resp,
                       n_inv=n_inv, pocos=0 < n_resp < MIN_RESPUESTAS_CONFIABLES, hay_datos=n_resp > 0,
                       pagina=pagina, paginas=paginas, por_pagina=por_pagina, total_coment=total_coment,
                       desde=desde, hasta=hasta, opciones_pagina=(10, 25, 50, 100), puede_editar=_puede_editar(),
                       n_activas=sum(1 for p in preguntas if p["activa"]))

    # ── EDITOR DE PREGUNTAS ─────────────────────────────────────────────
    def _exigir_editor():
        if not _puede_editar():
            return _json({"ok": False, "error": "Solo administración o la jefatura de Retiros edita las preguntas."}, 403)
        return None

    @app.route(f"{RUTA_BASE}/preguntas", methods=["GET"])
    @require_permission("retiros")
    def retiros_encuesta_editor():
        if not _puede_editar():
            abort(403)
        return _pagina("retiros/encuesta_editar.html", tipos=TIPOS, colores=COLORES_OPCION, activa=encuesta_activa(),
                       maximo_recomendado=MAX_PREGUNTAS_RECOMENDADO)

    def _datos_editor():
        preguntas = _preguntas_vigentes()
        for p in preguntas:
            c = mysql_fetchone("SELECT COUNT(*) AS c FROM pickup_encuesta_respuestas WHERE pregunta_id=%s", (p["id"],)) or {}
            p["respuestas"] = int(c.get("c") or 0)
        return {"ok": True, "preguntas": preguntas, "activas": sum(1 for p in preguntas if p["activa"]),
                "maximo_recomendado": MAX_PREGUNTAS_RECOMENDADO}

    @app.route(f"{RUTA_BASE}/preguntas/datos", methods=["GET"])
    @require_permission("retiros")
    def retiros_encuesta_datos():
        r = _exigir_editor()
        return r if r else _json(_datos_editor())

    @app.route(f"{RUTA_BASE}/preguntas/guardar", methods=["POST"])
    @require_permission("retiros")
    def retiros_encuesta_guardar():
        r = _exigir_editor()
        if r:
            return r
        cuerpo = request.get_json(silent=True) or {}
        limpia, errores = validar_definicion(cuerpo)
        if errores:
            return _json({"ok": False, "errores": errores}, 422)
        _asegurar_esquema()
        opciones_json = json.dumps(limpia["opciones"], ensure_ascii=False) if limpia["tipo"] == "opcion" else None
        pid = cuerpo.get("id")
        if pid:
            try:
                pid = int(pid)
            except (TypeError, ValueError):
                return _json({"ok": False, "error": "Pregunta no válida."}, 400)
            actual = mysql_fetchone("SELECT * FROM pickup_encuesta_preguntas WHERE id=%s AND vigente=1", (pid,))
            if not actual:
                return _json({"ok": False, "error": "Esa pregunta ya cambió o no existe. Recarga la pantalla."}, 404)
            ant = _fila_a_pregunta(actual)
            if (ant["texto"], ant["ayuda"], ant["tipo"], ant["opciones"], int(ant["obligatoria"])) == \
                    (limpia["texto"], limpia["ayuda"], limpia["tipo"], limpia["opciones"] if limpia["tipo"] == "opcion" else [],
                     limpia["obligatoria"]):
                return _json({"ok": True, "modo": "sin_cambios"})
            n_resp = int((mysql_fetchone("SELECT COUNT(*) AS c FROM pickup_encuesta_respuestas WHERE pregunta_id=%s",
                                         (pid,)) or {}).get("c") or 0)
            if n_resp == 0:
                # Nadie la ha respondido: se corrige en el mismo lugar (no se acumulan versiones mientras se diseña).
                _ejecutar("UPDATE pickup_encuesta_preguntas SET texto=%s, ayuda=%s, tipo=%s, opciones=%s, "
                          "obligatoria=%s WHERE id=%s AND vigente=1",
                          (limpia["texto"], limpia["ayuda"] or None, limpia["tipo"], opciones_json,
                           limpia["obligatoria"], pid))
                return _json({"ok": True, "modo": "editada"})
            # Ya tiene respuestas: VERSIÓN NUEVA. La anterior queda intacta para que esas respuestas sigan
            # apuntando al texto que vio el cliente.
            nueva = int(actual["version"]) + 1
            _ejecutar("INSERT INTO pickup_encuesta_preguntas (clave, version, texto, ayuda, tipo, opciones, obligatoria, "
                      "activa, orden, vigente, creado_en, creado_por) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s)",
                      (actual["clave"], nueva, limpia["texto"], limpia["ayuda"] or None, limpia["tipo"], opciones_json,
                       limpia["obligatoria"], actual["activa"], actual["orden"], _ahora(), _quien()))
            _ejecutar("UPDATE pickup_encuesta_preguntas SET vigente=0 WHERE id=%s", (pid,))
            return _json({"ok": True, "modo": "nueva_version", "version": nueva})
        fila = mysql_fetchone("SELECT COALESCE(MAX(orden),0) AS m FROM pickup_encuesta_preguntas WHERE vigente=1") or {}
        clave = "p" + secrets.token_hex(4)
        _ejecutar("INSERT INTO pickup_encuesta_preguntas (clave, version, texto, ayuda, tipo, opciones, obligatoria, "
                  "activa, orden, vigente, creado_en, creado_por) VALUES (%s,1,%s,%s,%s,%s,%s,1,%s,1,%s,%s)",
                  (clave, limpia["texto"], limpia["ayuda"] or None, limpia["tipo"], opciones_json,
                   limpia["obligatoria"], int(fila.get("m") or 0) + 10, _ahora(), _quien()))
        return _json({"ok": True, "modo": "creada"})

    @app.route(f"{RUTA_BASE}/preguntas/<int:pid>/activa", methods=["POST"])
    @require_permission("retiros")
    def retiros_encuesta_activa_toggle(pid):
        r = _exigir_editor()
        if r:
            return r
        _asegurar_esquema()
        activa = 1 if (request.get_json(silent=True) or {}).get("activa") in (True, 1, "1", "true") else 0
        n = _ejecutar("UPDATE pickup_encuesta_preguntas SET activa=%s WHERE id=%s AND vigente=1", (activa, pid))
        if not n and not mysql_fetchone("SELECT id FROM pickup_encuesta_preguntas WHERE id=%s AND vigente=1", (pid,)):
            return _json({"ok": False, "error": "Esa pregunta no existe o ya cambió de versión. Recarga."}, 404)
        return _json({"ok": True, "activa": bool(activa)})

    @app.route(f"{RUTA_BASE}/preguntas/<int:pid>/mover", methods=["POST"])
    @require_permission("retiros")
    def retiros_encuesta_mover(pid):
        r = _exigir_editor()
        if r:
            return r
        sentido = (request.get_json(silent=True) or {}).get("dir")
        if sentido not in ("arriba", "abajo"):
            return _json({"ok": False, "error": "Movimiento no válido."}, 400)
        filas = _preguntas_vigentes()
        ids = [p["id"] for p in filas]
        if pid not in ids:
            return _json({"ok": False, "error": "Esa pregunta no existe. Recarga."}, 404)
        i = ids.index(pid)
        j = i - 1 if sentido == "arriba" else i + 1
        if 0 <= j < len(ids):
            ids[i], ids[j] = ids[j], ids[i]
            orig = {p["id"]: p["orden"] for p in filas}
            for pos, qid in enumerate(ids):
                nuevo = (pos + 1) * 10
                if orig[qid] != nuevo:
                    _ejecutar("UPDATE pickup_encuesta_preguntas SET orden=%s WHERE id=%s", (nuevo, qid))
        return _json({"ok": True})

    # ── FICHA DEL RETIRO: enlace + estado (solo lectura; NO envía) ──────
    @app.route(f"{RUTA_BASE}/ficha/<int:rid>", methods=["GET"])
    @require_permission("retiros")
    def retiros_encuesta_ficha(rid):
        fila = mysql_fetchone(f"SELECT id, status, public_token, closed_at FROM `{REQ}` WHERE id=%s LIMIT 1", (rid,))
        if not fila:
            return _json({"ok": False, "error": "Retiro no encontrado."}, 404)
        ruta = ruta_para(fila["id"], fila.get("public_token") or "")
        base = (_base_publica() if callable(_base_publica) else "") or request.url_root.rstrip("/")
        out = {"ok": True, "activa": encuesta_activa(), "estado": "sin_invitacion",
               "estado_txt": "No enviada", "url": (base.rstrip("/") + ruta) if ruta else "",
               "elegible": fila.get("status") in ("retirada", "cerrada"), "status": fila.get("status"),
               "vence": "", "respuestas": []}
        try:
            _asegurar_esquema()
            inv = _invitacion_de(fila["id"])
            if inv:
                out["vence"] = _fmt_fecha(inv.get("vence_en"))
                if inv.get("respondida_en"):
                    out["estado"], out["estado_txt"] = "respondida", "Respondida"
                    out["respondida"] = _fmt_fecha(inv.get("respondida_en"))
                    filas = mysql_fetchall(
                        "SELECT p.texto AS texto, p.tipo AS tipo, p.opciones AS opciones, r.valor_num AS valor_num, "
                        "r.valor_texto AS valor_texto FROM pickup_encuesta_respuestas r "
                        "JOIN pickup_encuesta_preguntas p ON p.id=r.pregunta_id WHERE r.invitacion_id=%s "
                        "ORDER BY p.orden, p.id", (inv["id"],)) or []
                    for f in filas:
                        if f["tipo"] == "estrellas":
                            legible = f"{f['valor_num']} de 5"
                        elif f["tipo"] == "si_no":
                            legible = "Sí" if int(f["valor_num"]) == 1 else "No"
                        elif f["tipo"] == "opcion":
                            try:
                                ops = {o["valor"]: o["etiqueta"] for o in json.loads(f.get("opciones") or "[]")}
                            except ValueError:
                                ops = {}
                            legible = ops.get(f["valor_texto"], f["valor_texto"])
                        else:
                            legible = f["valor_texto"] or "(texto borrado por conservación)"
                        out["respuestas"].append({"pregunta": f["texto"], "valor": legible, "texto": f["tipo"] == "texto"})
                elif inv.get("enviada_en"):
                    out["estado"], out["estado_txt"] = "enviada", "Enviada, sin responder"
                else:
                    out["estado"], out["estado_txt"] = "creada", "Invitación creada, no enviada"
        except Exception as e:
            print(f"[ILUS][ENCUESTA] ficha: {type(e).__name__}", flush=True)
            out["estado_txt"] = "No se pudo leer el estado"
        return _json(out)

    # ── PÚBLICA ─────────────────────────────────────────────────────────
    @app.route(f"{RUTA_BASE}/<token>", methods=["GET", "POST"])
    def retiros_encuesta_publica(token):
        personal = _es_personal()
        activa = encuesta_activa()
        if not activa and not personal:
            return _no_disponible()          # sin lanzar: el cliente no puede abrirla
        retiro = _retiro_de_token(token)
        if not retiro:
            return _no_disponible()
        elegible = retiro.get("status") in ("retirada", "cerrada")
        if not personal and not elegible:
            return _no_disponible()
        # El personal NUNCA guarda con el token de un cliente real (taparía su respuesta): siempre vista previa.
        vista_previa = bool(personal)
        accion = url_for("retiros_encuesta_publica", token=token)
        inv = None
        if not vista_previa:
            try:
                inv = _obtener_o_crear_invitacion(retiro["id"], token, retiro.get("closed_at"))
            except Exception as e:
                print(f"[ILUS][ENCUESTA] invitación: {type(e).__name__}", flush=True)
                return _no_disponible()
            if not inv:
                return _no_disponible()
            if inv.get("respondida_en"):
                if request.method == "POST":
                    return redirect(accion, code=303)
                return _pagina("retiros/encuesta_gracias.html", ya_respondida=True)
            vence = _a_dt(inv.get("vence_en"))
            if vence and vence < _ahora():
                return _no_disponible()
        preguntas = _preguntas_vigentes(solo_activas=True)
        if not preguntas and not vista_previa:
            return _no_disponible()
        if request.method == "POST":
            respuestas, errores = validar_respuestas(preguntas, request.form)
            if errores:
                return _pagina("retiros/encuesta_publica.html", 422, vista_previa=vista_previa, preguntas=preguntas,
                               errores=errores, accion=accion, datos_form=request.form)
            if vista_previa:
                return _pagina("retiros/encuesta_gracias.html", vista_previa=True)
            try:
                ahora = _ahora()
                # «Reclamo» atómico: solo quien cambia respondida_en de NULL a una fecha guarda las respuestas.
                if _reclamar(inv["id"], ahora):
                    try:
                        for r in respuestas:
                            _ejecutar("INSERT IGNORE INTO pickup_encuesta_respuestas (invitacion_id, pregunta_id, "
                                      "valor_num, valor_texto, creado_en) VALUES (%s,%s,%s,%s,%s)",
                                      (inv["id"], r["pregunta_id"], r["valor_num"], r["valor_texto"], ahora))
                    except Exception:
                        # Si no se pudieron guardar, se libera el reclamo para que pueda reintentar.
                        _ejecutar("UPDATE pickup_encuesta_invitaciones SET respondida_en=NULL, consentimiento_en=NULL, "
                                  "version_aviso=NULL WHERE id=%s", (inv["id"],))
                        raise
            except Exception as e:
                # Nada de detalles internos al cliente (REGLA #4); sin params en el log (pueden traer datos personales).
                print(f"[ILUS][ENCUESTA] no pude guardar: {type(e).__name__}", flush=True)
                return _pagina("retiros/encuesta_publica.html", 500, vista_previa=False, preguntas=preguntas,
                               errores={"_general": "No pudimos guardar tus respuestas. Inténtalo de nuevo en un momento."},
                               accion=accion, datos_form=request.form)
            return redirect(accion, code=303)
        return _pagina("retiros/encuesta_publica.html", vista_previa=vista_previa, preguntas=preguntas, accion=accion)

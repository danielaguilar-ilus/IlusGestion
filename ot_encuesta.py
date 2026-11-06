# -*- coding: utf-8 -*-
"""ENCUESTA CORTA DE LA ORDEN DE TRABAJO (Daniel, 6-nov-2026) — REGLA #68.

Daniel: «podemos agregar una encuesta [al correo de OT terminada] con elegancia y sin aburrir con las respuestas, por favor».

UNA pregunta («¿Cómo fue tu servicio?», del 1 al 5) y un comentario opcional de hasta 300 caracteres. Se contesta con UN toque desde el correo
«OT terminada» (o desde «Enviar OT por correo»): cada número del correo es un enlace a `/ot-encuesta/<token>?n=<1..5>`.

LO QUE ESTE MÓDULO HACE
  · Token HMAC SIN estado, con su propio propósito (no es el de la firma remota ni el de Retiros): ata el enlace a la OT y a ESA aprobación del cierre.
  · Valida la nota (1 a 5) y limpia el comentario (nada de caracteres de control ni de dirección de texto).
  · Arma el bloque del correo A PRUEBA DE OUTLOOK DE ESCRITORIO (tablas con `bgcolor`, el relleno en el `<td>`, sin imágenes, sin `padding` ni `margin`
    en `<table>`/`<a>`).
  · Registra las rutas: la página pública (`/ot-encuesta/<token>`, GET no guarda nada; POST guarda una sola vez) y los resultados para el equipo
    interno (`/ot/encuestas`, nunca un técnico).

PRIVACIDAD (Ley 19.628 modificada por la 21.719): la respuesta se guarda con la OT, sin nombre, sin correo y sin dirección IP; se conserva
MESES_CONSERVACION meses. El comentario se guarda como texto y se muestra SIEMPRE escapado.

REGLA #23 / #22: el bloque solo viaja en los correos REALES con `OT_ENCUESTA_ACTIVA=1` (nace APAGADA; no está en deploy.yml). La vista previa de Control de
correos y «Enviarme una prueba» lo muestran siempre, con el token «vista-previa» (esa página no guarda nada).

Pruebas: tests/test_ot_encuesta.py.
"""
import base64
import hashlib
import hmac
import html as _html
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone

# Flask se importa DENTRO de `register_ot_encuesta_routes`: el resto del módulo es puro y lo importan `comm_correos_registro` (la vista previa) y las pruebas.

# ── Identificadores ──────────────────────────────────────────────────────────────────────────────────────────────────────
ENV_ACTIVA = "OT_ENCUESTA_ACTIVA"          # nace APAGADA; encenderla requiere el «sí» explícito de Daniel (REGLAS #22 y #23)
RUTA_BASE = "/ot-encuesta"                 # página pública
RUTA_RESULTADOS = "/ot/encuestas"          # resultados internos
TOKEN_VISTA_PREVIA = "vista-previa"        # token de la página de ejemplo (no guarda nada)
TOKEN_DIAS = 45                            # vigencia del enlace del correo
MAX_COMENTARIO = 300
MESES_CONSERVACION = 24
AVISO_VERSION = "2026-11-06"
CONTACTO_DERECHOS = "soportetec@sphs.cl"
RESPONSABLE_RAZON = "Sport and Health Solutions SPA"      # razón social (documento con efecto legal: REGLA #0)
AVISO_FINALIDAD = "mejorar el servicio técnico de ILUS Fitness"
SOPORTE_URL_POR_DEFECTO = "https://ilusfitness.com/pages/soporte-tecnico"
NOTA_BAJA_MAX = 2                          # 1 y 2: «Lamentamos no haber estado a la altura» + botón del ticket
POR_PAGINA = (10, 25, 50, 100)
PERIODOS = (("30", "Últimos 30 días", 30), ("90", "Últimos 90 días", 90), ("365", "Último año", 365), ("todo", "Todo", None))

# La escala 1–5: etiqueta, cara (solo en la página) y colores del correo (relleno suave y texto oscuro: contraste alto, de rojo suave a verde).
ESCALA = {
    1: {"etiqueta": "Mala", "cara": "\U0001F61E", "bg": "#fecaca", "fg": "#6b1313", "borde": "#f87171"},
    2: {"etiqueta": "Regular", "cara": "\U0001F641", "bg": "#fed7aa", "fg": "#6c2610", "borde": "#fb923c"},
    3: {"etiqueta": "Buena", "cara": "\U0001F610", "bg": "#fde68a", "fg": "#78350f", "borde": "#fbbf24"},
    4: {"etiqueta": "Muy buena", "cara": "\U0001F642", "bg": "#d9f99d", "fg": "#365314", "borde": "#a3e635"},
    5: {"etiqueta": "Excelente", "cara": "\U0001F929", "bg": "#86efac", "fg": "#0f3d22", "borde": "#22c55e"},
}

SQL_TABLA = """
    CREATE TABLE IF NOT EXISTS ot_encuesta_respuestas (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        visita_id     INT NOT NULL,
        cierre_clave  VARCHAR(40) NOT NULL,
        puntaje       TINYINT NOT NULL,
        comentario    VARCHAR(300) NULL,
        aviso_version VARCHAR(12) NULL,
        creada_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_ot_encuesta (visita_id, cierre_clave),
        KEY idx_ot_encuesta_creada (creada_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_VERDADERO = {"1", "true", "yes", "on", "si", "sí"}


def encuesta_activa(environ=None):
    """¿Viaja el bloque de la encuesta en los correos REALES? Solo con `OT_ENCUESTA_ACTIVA` en 1/true/yes/on/si. Ausente o vacía = apagada."""
    env = os.environ if environ is None else environ
    return (env.get(ENV_ACTIVA) or "").strip().lower() in _VERDADERO


def secreto_de(app):
    """La clave del servidor para firmar los enlaces (propia si existe `ILUS_OT_ENCUESTA_SECRET`; si no, la de la app)."""
    s = os.environ.get("ILUS_OT_ENCUESTA_SECRET") or getattr(app, "secret_key", None) or ""
    return s.decode("utf-8", "ignore") if isinstance(s, (bytes, bytearray)) else str(s)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  TOKEN (HMAC sin estado, con su propio propósito)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
_PROPOSITO = b"ilus:ot-encuesta:v1"
_RX_TOKEN = re.compile(r"^[A-Za-z0-9_\-]{8,160}\.[0-9a-f]{24}$")


def _clave_derivada(secreto):
    """La clave del token sale de la del servidor + el propósito: el token de la encuesta NO sirve como el de la firma remota ni al revés."""
    return hmac.new(str(secreto).encode("utf-8"), _PROPOSITO, hashlib.sha256).digest()


def _firmar(secreto, cuerpo):
    return hmac.new(_clave_derivada(secreto), cuerpo.encode("ascii"), hashlib.sha256).hexdigest()[:24]


def generar_token(secreto, vid, cierre_clave, ahora=None, dias=TOKEN_DIAS):
    """`<payload en base64url>.<24 hex de HMAC-SHA256>`. El payload lleva la OT, el instante de vencimiento y la clave de ESA aprobación del cierre
    (`mant_ot_comunicaciones.cierre_clave`): si la OT se reabre y se aprueba de nuevo es otra aprobación y otra respuesta. '' si falta la clave o la OT."""
    try:
        vid = int(vid)
    except (TypeError, ValueError):
        return ""
    if not secreto or vid <= 0 or not str(cierre_clave or "").strip():
        return ""
    exp = int((time.time() if ahora is None else ahora)) + int(dias) * 86400
    cuerpo = base64.urlsafe_b64encode(f"v1|{vid}|{exp}|{str(cierre_clave).strip()[:40]}".encode("utf-8")).decode("ascii").rstrip("=")
    return f"{cuerpo}.{_firmar(secreto, cuerpo)}"


def validar_token(secreto, token, ahora=None):
    """{ok, motivo, vid, cierre_clave}. motivo: 'invalido' (no es nuestro, está adulterado o no se puede leer) | 'vencido' (la firma es buena pero pasaron
    los días). Nunca lanza."""
    malo = {"ok": False, "motivo": "invalido", "vid": None, "cierre_clave": None}
    try:
        t = str(token or "")
        if not secreto or not _RX_TOKEN.match(t):
            return malo
        cuerpo, firma = t.rsplit(".", 1)
        if not hmac.compare_digest(_firmar(secreto, cuerpo), firma):
            return malo
        crudo = base64.urlsafe_b64decode(cuerpo + "=" * (-len(cuerpo) % 4)).decode("utf-8")
        version, vid_s, exp_s, clave = crudo.split("|", 3)
        if version != "v1" or not clave.strip():
            return malo
        vid, exp = int(vid_s), int(exp_s)
        if vid <= 0:
            return malo
    except Exception:
        return malo
    if (time.time() if ahora is None else ahora) > exp:
        return {"ok": False, "motivo": "vencido", "vid": vid, "cierre_clave": clave}
    return {"ok": True, "motivo": "", "vid": vid, "cierre_clave": clave}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  La respuesta: nota y comentario
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def puntaje_valido(valor):
    """1..5 desde el texto del formulario o de la URL («3»). Cualquier otra cosa (vacío, «3.5», «07», «５», «+3») → None."""
    t = str(valor if valor is not None else "").strip()
    if len(t) == 1 and t in "12345":
        return int(t)
    return None


def limpiar_comentario(texto):
    """El comentario como texto plano: sin caracteres de control, sin marcas de dirección del texto (se usan para disfrazar lo que se lee), con saltos
    de línea normales, sin exceso de líneas en blanco y recortado a MAX_COMENTARIO. NO se escapa acá: se guarda tal cual y se ESCAPA al mostrarlo."""
    t = str(texto or "").replace("\r\n", "\n").replace("\r", "\n")
    salida = []
    for ch in t:
        cat = unicodedata.category(ch)
        if ch in ("\n", "\t"):
            salida.append(ch if ch == "\n" else " ")
        elif cat in ("Cc", "Cs", "Co", "Cn"):
            continue                                   # control, sustitutos, uso privado y sin asignar
        elif cat in ("Zl", "Zp"):
            salida.append("\n")                        # separadores de línea y de párrafo de Unicode: un salto de línea normal
        elif cat == "Cf" and ch != "‍":
            continue                                   # formato invisible: espacio de ancho cero, marcas de dirección (U+202A–202E, U+2066–2069).
            #                                            Se conserva el unidor de ancho cero (U+200D) que arma los emojis compuestos (👨‍👩‍👧).
        else:
            salida.append(ch)
    t = "".join(salida)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n[ ]+", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t[:MAX_COMENTARIO].rstrip()


def validar_respuesta(puntaje, comentario):
    """(nota|None, comentario_limpio, errores). Sin nota no se guarda nada; el comentario es opcional."""
    n = puntaje_valido(puntaje)
    errores = {}
    if n is None:
        errores["puntaje"] = "Elige una nota del 1 al 5."
    return n, limpiar_comentario(comentario), errores


def tono_gracias(puntaje):
    """Cómo suena el agradecimiento: «baja» (1–2: lamentamos y ofrecemos el ticket), «media» (3) o «alta» (4–5)."""
    try:
        n = int(puntaje)
    except (TypeError, ValueError):
        return "alta"
    return "baja" if n <= NOTA_BAJA_MAX else ("media" if n == 3 else "alta")


def estrellas_txt(puntaje):
    """★★★★☆ — la nota en texto (se lee igual en la página, el correo y el Excel)."""
    try:
        n = max(0, min(5, int(puntaje)))
    except (TypeError, ValueError):
        n = 0
    return "★" * n + "☆" * (5 - n)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  El bloque del correo — A PRUEBA DE OUTLOOK DE ESCRITORIO
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Outlook de escritorio usa el motor de Word: ignora `padding`/`margin` en <table> y en <a>, muchas veces `border-radius`, bloquea las imágenes y no entiende
# flex/grid. Por eso: tablas con role="presentation" y cellpadding/cellspacing/border en cero; CADA botón es un <td> con `bgcolor` + `width` + `height` y el
# relleno en el <td>; el <a> de adentro es `display:block`, con el color y SIN relleno; el espacio vertical lo da el padding de un <td>; todo el texto
# lleva su `font-family`; sin imágenes (los números van como texto); el ancho total nunca pasa de 520 px y en el celular las cinco celdas siguen en una fila
# (cada una ~20 %).
_F = "font-family:Arial,Helvetica,sans-serif;"
ANCHO_MAX = 520


def _celda_nota(n, url_base):
    e = ESCALA[n]
    sep = "&" if "?" in url_base else "?"
    href = _html.escape(f"{url_base}{sep}n={n}", quote=True)
    titulo = f"Calificar con {n} de 5: {e['etiqueta']}"
    return (
        '<td width="20%" align="center" valign="top" style="width:20%;padding:0 3px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td width="100%" height="48" align="center" valign="middle" bgcolor="{e["bg"]}" '
        f'style="background:{e["bg"]};height:48px;border:1px solid {e["borde"]};border-radius:8px">'
        f'<a href="{href}" target="_blank" title="{_html.escape(titulo, quote=True)}" aria-label="{_html.escape(titulo, quote=True)}" '
        f'style="display:block;{_F}font-size:22px;line-height:46px;font-weight:bold;color:{e["fg"]};text-decoration:none">{n}</a>'
        '</td></tr></table></td>')


def bloque_html(url_base):
    """El bloque «¿Cómo fue tu servicio?» del correo: cinco números, de rojo suave a verde, cada uno un enlace a `url_base?n=N`. `url_base` es la URL
    ABSOLUTA de la página (`https://…/ot-encuesta/<token>`). Sin URL → '' (el bloque no se dibuja). Todo escapado."""
    url_base = str(url_base or "").strip()
    if not url_base:
        return ""
    celdas = "".join(_celda_nota(n, url_base) for n in range(1, 6))
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        '<td style="padding:22px 0 8px">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:{ANCHO_MAX}px"><tr>'
        f'<td bgcolor="#f5f5f7" style="background:#f5f5f7;border-left:4px solid #dc2626;padding:16px 14px 16px 16px;{_F}">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'<tr><td style="{_F}font-size:12px;line-height:16px;font-weight:bold;letter-spacing:1px;color:#4b5563;padding:0 0 4px">TU OPINIÓN</td></tr>'
        f'<tr><td style="{_F}font-size:19px;line-height:24px;font-weight:bold;color:#111827;padding:0 0 4px">¿Cómo fue tu servicio?</td></tr>'
        f'<tr><td style="{_F}font-size:14px;line-height:20px;color:#4b5563;padding:0 0 12px">Toca un número. Te toma 10 segundos.</td></tr>'
        '<tr><td>'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>{celdas}</tr></table>'
        '</td></tr>'
        '<tr><td style="padding:6px 3px 0">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td align="left" style="{_F}font-size:12px;line-height:16px;color:#4b5563">1 · Mala</td>'
        f'<td align="right" style="{_F}font-size:12px;line-height:16px;color:#4b5563">5 · Excelente</td>'
        '</tr></table></td></tr>'
        '</table></td></tr></table></td></tr></table>')


def enlace_publico(base_url, token):
    """`https://…/ot-encuesta/<token>` (sin la nota). '' si falta el token."""
    return f"{str(base_url or '').rstrip('/')}{RUTA_BASE}/{token}" if token else ""


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Resultados (equipo interno)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def resumen(distribucion):
    """Las cifras de la tarjeta «Satisfacción». `distribucion`: {nota: cantidad} (notas ausentes = 0). Devuelve total, promedio (1 decimal, None si no hay),
    % de satisfechos (4 y 5), cuántas están por revisar (1 y 2) y las barras de 1 a 5 (cada % sobre el total)."""
    d = {n: max(0, int((distribucion or {}).get(n, 0) or 0)) for n in range(1, 6)}
    total = sum(d.values())
    promedio = round(sum(n * c for n, c in d.items()) / total, 1) if total else None
    barras = [{"nota": n, "cantidad": d[n], "pct": (round(100 * d[n] / total) if total else 0),
               "etiqueta": ESCALA[n]["etiqueta"], "bg": ESCALA[n]["bg"], "borde": ESCALA[n]["borde"]} for n in range(5, 0, -1)]
    return {"total": total, "promedio": promedio,
            "satisfechos_pct": (round(100 * (d[4] + d[5]) / total) if total else None),
            "por_revisar": d[1] + d[2], "barras": barras}


def semaforo(promedio):
    """verde (≥ 4,3) · ámbar (≥ 3,6) · rojo: los mismos umbrales de la encuesta de Retiros."""
    if promedio is None:
        return "gris"
    return "verde" if promedio >= 4.3 else ("ambar" if promedio >= 3.6 else "rojo")


def _entero(v, defecto, minimo, maximo):
    try:
        n = int(str(v).strip())
    except (TypeError, ValueError):
        return defecto
    return max(minimo, min(maximo, n))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Rutas
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def register_ot_encuesta_routes(app, ctx):
    """Registra la página pública y los resultados internos. `ctx` = `globals()` de app.py (o un diccionario equivalente en las pruebas)."""
    from flask import make_response, render_template, request, url_for
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    con_rowcount = ctx.get("mysql_execute_returning_rowcount")
    _mant_required = ctx["_mant_required"]
    _no_tecnico = ctx["_no_tecnico"]
    rl_check = ctx.get("_rl_check")
    soporte_url = ctx.get("ILUS_SOPORTE_URL") or SOPORTE_URL_POR_DEFECTO
    tipos_ot = ctx.get("_TIPO_OT_LABEL") or {}
    estado = {"tabla": False}

    def _ejecutar(sql, params=()):
        if con_rowcount:
            return int(con_rowcount(sql, params) or 0)
        return int(mysql_execute(sql, params) or 0)

    def _asegurar_tabla():
        """Respaldo del arranque: UN solo CREATE TABLE IF NOT EXISTS (barato si ya existe: REGLA #18), una vez por proceso."""
        if not estado["tabla"]:
            mysql_execute(SQL_TABLA)
            estado["tabla"] = True

    def _con_cabeceras(resp):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    def _pagina(plantilla, codigo=200, **kw):
        kw.setdefault("escala", ESCALA)
        kw.setdefault("max_comentario", MAX_COMENTARIO)
        kw.setdefault("soporte_url", soporte_url)
        kw.setdefault("vista_previa", False)
        kw.setdefault("aviso", {"version": AVISO_VERSION, "razon": RESPONSABLE_RAZON, "contacto": CONTACTO_DERECHOS,
                                "finalidad": AVISO_FINALIDAD, "meses": MESES_CONSERVACION})
        return _con_cabeceras(make_response(render_template(plantilla, **kw), codigo))

    def _no_disponible(vencido=False):
        return _pagina("ot_encuesta/no_disponible.html", 410, vencido=vencido)

    def _ot_de(vid):
        try:
            return mysql_fetchone("SELECT id, numero_ot, estado FROM mant_visitas WHERE id=%s LIMIT 1", (int(vid),))
        except Exception as e:
            print(f"[ILUS][OT-ENCUESTA] OT: {type(e).__name__}", flush=True)
            return None

    def _respuesta_de(vid, clave):
        return mysql_fetchone("SELECT puntaje, comentario FROM ot_encuesta_respuestas WHERE visita_id=%s AND cierre_clave=%s LIMIT 1",
                              (int(vid), clave))

    # ── PÚBLICA ─────────────────────────────────────────────────────────────────────────────────────────────────
    @app.route(f"{RUTA_BASE}/<token>", methods=["GET", "POST"])
    def ot_encuesta_publica(token):
        """GET: muestra la nota ya marcada (si viene `?n=`) y el comentario opcional. NO guarda nada (los escáneres de correo abren los enlaces).
        POST: guarda la respuesta UNA sola vez por OT y aprobación del cierre; la segunda vez dice «ya recibimos tu respuesta»."""
        if token == TOKEN_VISTA_PREVIA:
            return _vista_previa()
        v = validar_token(secreto_de(app), token)
        if not v["ok"]:
            return _no_disponible(vencido=(v["motivo"] == "vencido"))
        if request.method == "POST" and rl_check is not None:
            try:
                if not rl_check("ot_encuesta", 20, 3600):
                    return _pagina("ot_encuesta/no_disponible.html", 429, vencido=False, demasiados=True)
            except Exception as e:
                print(f"[ILUS][OT-ENCUESTA] límite: {type(e).__name__}", flush=True)
        ot = _ot_de(v["vid"])
        if not ot:
            return _no_disponible()
        numero = ot.get("numero_ot") or ""
        try:
            previa = _respuesta_de(v["vid"], v["cierre_clave"])
        except Exception:
            previa = None            # la tabla aún no existe: nadie ha respondido
        if previa:
            return _pagina("ot_encuesta/gracias.html", ya_respondida=True, numero=numero, tono=tono_gracias(previa.get("puntaje")))
        accion = url_for("ot_encuesta_publica", token=token)
        if request.method == "POST":
            nota, comentario, errores = validar_respuesta(request.form.get("n"), request.form.get("comentario"))
            if errores:
                return _pagina("ot_encuesta/publica.html", 422, numero=numero, accion=accion, nota=nota, comentario=comentario,
                               errores=errores)
            try:
                _asegurar_tabla()
                ganadora = _ejecutar(
                    "INSERT IGNORE INTO ot_encuesta_respuestas (visita_id, cierre_clave, puntaje, comentario, aviso_version) "
                    "VALUES (%s,%s,%s,%s,%s)", (int(v["vid"]), v["cierre_clave"], nota, comentario or None, AVISO_VERSION)) == 1
            except Exception as e:
                # Nada de detalles internos al cliente (REGLA #4), y sin los parámetros en el log (traen lo que escribió la persona).
                print(f"[ILUS][OT-ENCUESTA] no pude guardar: {type(e).__name__}", flush=True)
                return _pagina("ot_encuesta/publica.html", 500, numero=numero, accion=accion, nota=nota, comentario=comentario,
                               errores={"_general": "No pudimos guardar tu respuesta. Inténtalo de nuevo en un momento."})
            if not ganadora:
                # otra petición (o el mismo toque repetido) ya guardó la respuesta de esta OT: se dice con la verdad
                previa = _respuesta_de(v["vid"], v["cierre_clave"]) or {}
                return _pagina("ot_encuesta/gracias.html", ya_respondida=True, numero=numero, tono=tono_gracias(previa.get("puntaje")))
            return _pagina("ot_encuesta/gracias.html", ya_respondida=False, numero=numero, tono=tono_gracias(nota))
        return _pagina("ot_encuesta/publica.html", numero=numero, accion=accion, nota=puntaje_valido(request.args.get("n")), comentario="",
                       errores={})

    def _vista_previa():
        """La página de ejemplo (el token «vista-previa» va en los correos de prueba y en la vista previa de Control de correos): nada se guarda."""
        accion = url_for("ot_encuesta_publica", token=TOKEN_VISTA_PREVIA)
        if request.method == "POST":
            nota, comentario, errores = validar_respuesta(request.form.get("n"), request.form.get("comentario"))
            if errores:
                return _pagina("ot_encuesta/publica.html", 422, vista_previa=True, numero="OT-2026-00123", accion=accion, nota=nota,
                               comentario=comentario, errores=errores)
            return _pagina("ot_encuesta/gracias.html", vista_previa=True, ya_respondida=False, numero="OT-2026-00123", tono=tono_gracias(nota))
        return _pagina("ot_encuesta/publica.html", vista_previa=True, numero="OT-2026-00123", accion=accion,
                       nota=puntaje_valido(request.args.get("n")), comentario="", errores={})

    # ── RESULTADOS INTERNOS (nunca un técnico: REGLAS #19 y #26) ───────────────────────────────────────────────
    @app.route(RUTA_RESULTADOS, methods=["GET"])
    @_mant_required
    @_no_tecnico
    def ot_encuestas_pagina():
        """«Satisfacción» de las órdenes de trabajo: promedio, cuántas respuestas, barras de 1 a 5 y los últimos comentarios con su OT y su cliente."""
        per_page = _entero(request.args.get("per_page"), 10, 1, 100)
        if per_page not in POR_PAGINA:
            per_page = 10
        page = _entero(request.args.get("page"), 1, 1, 100000)
        periodo = request.args.get("periodo") or "90"
        dias = next((d for c, _t, d in PERIODOS if c == periodo), 90)
        if periodo not in [c for c, _t, _d in PERIODOS]:
            periodo = "90"
        filtro = request.args.get("nota") or ""
        solo_comentarios = request.args.get("comentarios") == "1"

        donde, params = [], []
        if dias:
            donde.append("r.creada_at >= %s")           # `creada_at` es UTC (CURRENT_TIMESTAMP del servidor de la base)
            params.append(datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=int(dias)))
        base_donde = (" WHERE " + " AND ".join(donde)) if donde else ""
        dist, filas, total_lista, error = {}, [], 0, False
        try:
            for r in (mysql_fetchall(
                    "SELECT r.puntaje AS nota, COUNT(*) AS n FROM ot_encuesta_respuestas r" + base_donde + " GROUP BY r.puntaje",
                    tuple(params)) or []):
                dist[int(r["nota"])] = int(r["n"])
            d2, p2 = list(donde), list(params)
            if filtro in ("1", "2", "3", "4", "5"):
                d2.append("r.puntaje=%s")
                p2.append(int(filtro))
            elif filtro == "bajas":
                d2.append("r.puntaje<=%s")
                p2.append(NOTA_BAJA_MAX)
            elif filtro == "altas":
                d2.append("r.puntaje>=4")
            if solo_comentarios:
                d2.append("r.comentario IS NOT NULL AND r.comentario<>''")
            w2 = (" WHERE " + " AND ".join(d2)) if d2 else ""
            total_lista = int((mysql_fetchone("SELECT COUNT(*) AS n FROM ot_encuesta_respuestas r" + w2, tuple(p2)) or {}).get("n") or 0)
            paginas = max(1, -(-total_lista // per_page))
            page = min(page, paginas)
            filas = mysql_fetchall(
                "SELECT r.id, r.visita_id, r.puntaje, r.comentario, r.creada_at, v.numero_ot, v.tipo, v.fecha_realizada, c.razon_social "
                "  FROM ot_encuesta_respuestas r LEFT JOIN mant_visitas v ON v.id = r.visita_id LEFT JOIN mant_clientes c ON c.id = v.cliente_id"
                + w2 + " ORDER BY r.creada_at DESC, r.id DESC LIMIT %s OFFSET %s", tuple(p2) + (per_page, (page - 1) * per_page)) or []
        except Exception as e:
            print(f"[ILUS][OT-ENCUESTA] resultados: {type(e).__name__}", flush=True)
            error = True
        paginas = max(1, -(-total_lista // per_page))
        cifras = resumen(dist)
        filas = [dict(f, estrellas=estrellas_txt(f.get("puntaje")), tipo_txt=tipos_ot.get(str(f.get("tipo") or "").lower(), f.get("tipo") or ""))
                 for f in filas]
        return _con_cabeceras(make_response(render_template(
            "ot2/encuestas.html", cifras=cifras, semaforo=semaforo(cifras["promedio"]), filas=filas, total_lista=total_lista,
            page=page, paginas=paginas, per_page=per_page, por_pagina=POR_PAGINA, periodo=periodo, periodos=PERIODOS, filtro=filtro,
            solo_comentarios=solo_comentarios, error=error, activa=encuesta_activa(), meses=MESES_CONSERVACION, escala=ESCALA)))

    app.extensions["ot_encuesta"] = {"secreto": lambda: secreto_de(app), "activa": encuesta_activa}

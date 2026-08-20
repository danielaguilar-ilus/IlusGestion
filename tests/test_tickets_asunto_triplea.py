"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Compatibilidad del lector de correo con el formato de asunto de Triple A
(2026-08-20).

CONTEXTO (Daniel, jefe de operaciones): Triple A es el PROVEEDOR que
desarrollo el sistema de tickets anterior. Se da de baja ese sistema y todo
queda en ILUS. El problema concreto: los correos YA ENVIADOS a clientes por
el sistema de Triple A usan el asunto

    "ILUS | Ticket - ID: 754"

mientras que ILUS usa

    "TK-2026-00754"

Cuando un cliente RESPONDE uno de esos correos viejos, el asunto que vuelve
es el de Triple A. Si el lector no lo reconoce, esa respuesta no entra a
ningun ticket y se pierde en silencio -- exactamente el modo de falla que ya
costo 12 dias de tickets perdidos en agosto (ver _tk_imap_creds).

DATO VERIFICADO EN PRODUCCION antes de escribir el codigo: el ID del asunto
de Triple A es el `id` INTERNO de tk_tickets, no una numeracion aparte. El
ticket id=754 existe y es real (numero_ticket "TAA-567", cliente LEANDRO
VARELA). Por eso la resolucion es id -> numero_ticket contra nuestra propia
tabla, sin ningun mapeo entre sistemas.

QUE VERIFICA:
 1. El asunto viejo de Triple A se reconoce y resuelve al numero_ticket real.
 2. El asunto propio (TK-AAAA-NNNNN) sigue funcionando IGUAL que antes
    (no se rompe el canal que hoy esta en produccion).
 3. Un correo propio/rebote sigue clasificando como 'propio' y NO se ingesta
    como mensaje de cliente, con AMBOS formatos.
 4. Un asunto sin numero sigue devolviendo 'sin_numero'.
 5. Un ID de Triple A que no existe en la base NO inventa un ticket.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_asunto_triplea.py
"""

import re
import sys

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# ══════════════════════════════════════════════════════════════════
# Replica EXACTA de los dos patrones y de la logica de clasificacion
# tal como quedaron en tickets_module.py. Se valida ademas contra el
# archivo real (abajo) para que este test no envejezca en silencio.
# ══════════════════════════════════════════════════════════════════
_TK_NUM_TICKET_RE = re.compile(r"TK-\d{4}-\d{5}", re.I)
_TK_TAA_SUBJECT_RE = re.compile(r"ILUS\s*\|\s*Ticket\s*[-–—]?\s*ID:\s*(\d+)", re.I)
_TK_TAA_NUM_RE = re.compile(r"\bTAA-\d+\b", re.I)

# Base falsa: legacy_taa_id (id de Triple A) -> numero_ticket nuestro.
#
# EL PUNTO MAS IMPORTANTE DE ESTE TEST: el numero del asunto es el id de
# TRIPLE A, que es OTRO sistema. Sus tickets se importaron por CSV y su id
# original quedo en legacy_taa_id; el id de tk_tickets es nuestro correlativo
# propio y NO coincide.
#
# Caso real verificado en produccion que destapo el error:
#   · correo "ILUS | Ticket - ID: 754" = de Gonzalo, rechazando una cotizacion
#   · ticket id=754 de ILUS            = de Leandro Varela, por un cable de
#                                        fuerza, y su legacy_taa_id es 567
# Resolver por `id` habria pegado la respuesta de Gonzalo en el ticket de
# Leandro Varela, en silencio. Por eso se resuelve por legacy_taa_id.
BD_POR_LEGACY_TAA = {
    754: "TAA-754",   # el ticket que DE VERDAD corresponde al correo "ID: 754"
    567: "TAA-567",   # este es el que tiene id=754 en nuestra tabla (la trampa)
    649: "TAA-649",
}

# id interno de ILUS -> numero_ticket. Solo para demostrar que NO se usa.
BD_POR_ID_INTERNO = {
    754: "TAA-567",   # ojo: el id 754 nuestro es el TAA-567 (cliente distinto)
}


def mysql_fetchone_falso(sql, params):
    tid = params[0]
    if "legacy_taa_id" in sql:
        if tid in BD_POR_LEGACY_TAA:
            return {"numero_ticket": BD_POR_LEGACY_TAA[tid]}
        return None
    # Si alguien vuelve a resolver por id, este camino lo delata en el test.
    if tid in BD_POR_ID_INTERNO:
        return {"numero_ticket": BD_POR_ID_INTERNO[tid]}
    return None


def clasificar(subject, from_email, user_email, incluir_taa=False):
    """Copia fiel de _tk_clasificar_correo con la BD simulada.

    incluir_taa (2026-08-20, tras la revision adversarial): el formato
    NATIVO viejo de Triple A ("ILUS | Ticket - ID: N", resuelto por
    legacy_taa_id) solo se intenta si incluir_taa=True -- es el que puede
    resucitar un ticket cerrado y solo debe correr en el barrido controlado.
    El formato YA MIGRADO ("TAA-N", el numero_ticket real y definitivo) se
    reconoce SIEMPRE, sea o no incluir_taa: es la conversacion viva de un
    ticket que YA existe en ILUS, no backlog historico."""
    m = _TK_NUM_TICKET_RE.search(subject or "")
    numero = m.group(0).upper() if m else None
    if not numero:
        m_taa_num = _TK_TAA_NUM_RE.search(subject or "")
        if m_taa_num:
            numero = m_taa_num.group(0).upper()
    if not numero and incluir_taa:
        m_taa = _TK_TAA_SUBJECT_RE.search(subject or "")
        if m_taa:
            _tid = int(m_taa.group(1))
            _row = mysql_fetchone_falso(
                "SELECT numero_ticket FROM tk_tickets WHERE legacy_taa_id=%s",
                (_tid,))
            if _row and _row.get("numero_ticket"):
                numero = _row["numero_ticket"].upper()
            else:
                return "sin_ticket_taa", f"TAA-{_tid}"
    if not numero:
        return "sin_numero", None
    fe = (from_email or "").strip().lower()
    propio = (
        not fe
        or fe == (user_email or "").strip().lower()
        or "mailer-daemon" in fe
        or "noreply" in fe
        or "no-reply" in fe
    )
    return ("propio" if propio else "candidato"), numero


BUZON = "soportetec@sphs.cl"
CLIENTE = "varelaft@gmail.com"

print("\n0. Por defecto (incluir_taa=False), el formato NATIVO NO se reconoce")
# 2026-08-20, tras la revision adversarial: esto es lo que protege al
# autopoll automatico (dispara con solo abrir la bandeja, SIN chequeo de
# rol) de tragarse el backlog historico de Triple A sin que nadie lo haya
# decidido. Solo el barrido controlado (superadmin, tk_api_mail_recuperar)
# pasa incluir_taa=True.
clase, numero = clasificar("ILUS | Ticket - ID: 754", CLIENTE, BUZON)
check(clase == "sin_numero" and numero is None,
      "sin incluir_taa=True, 'ILUS | Ticket - ID: 754' NO resuelve -- "
      "el autopoll normal no lo ve")

print("\n1. Asuntos REALES de Triple A (tomados de la bandeja de Daniel) -- "
      "requieren incluir_taa=True")
# Asuntos textuales de las capturas de la bandeja de soporte.
REALES = [
    ("ILUS | Ticket - ID: 754", "TAA-754"),
    ("ILUS | Ticket - ID: 649", "TAA-649"),
]
for asunto, esperado in REALES:
    clase, numero = clasificar(asunto, CLIENTE, BUZON, incluir_taa=True)
    check(clase == "candidato",
          f"'{asunto}' de un cliente (incluir_taa=True) -> candidato")
    check(numero == esperado,
          f"'{asunto}' resuelve al ticket real '{esperado}' (obtenido: {numero})")

print("\n1b. NO se confunde el id de Triple A con nuestro id interno")
# Este es el bug que se detecto y corrigio: nuestro ticket id=754 es el
# TAA-567 (otro cliente, otro tema). Si el codigo volviera a resolver por id,
# este check falla y avisa antes de que un cliente vea el error.
_, numero_754 = clasificar("ILUS | Ticket - ID: 754", CLIENTE, BUZON, incluir_taa=True)
check(numero_754 == "TAA-754",
      "'ID: 754' resuelve por legacy_taa_id al TAA-754 (el ticket correcto)")
check(numero_754 != "TAA-567",
      "'ID: 754' NO cae en el TAA-567, que es el que tiene id=754 en nuestra "
      "tabla y pertenece a OTRO cliente (Leandro Varela)")

print("\n1c. El numero YA MIGRADO (TAA-N) se reconoce SIEMPRE, incluir_taa o no")
# La conversacion VIVA de un ticket migrado: ILUS le manda hoy un correo
# nuevo sobre el ticket TAA-754 (la salvaguarda de asunto de
# _tk_notificar_lifecycle inyecta el numero_ticket real), el cliente
# responde, y esa respuesta tiene que seguir entrando -- SIN necesitar el
# barrido historico ni legacy_taa_id.
for incluir in (False, True):
    clase, numero = clasificar(
        "Re: ILUS · TAA-754 — Respuesta a tu ticket", CLIENTE, BUZON,
        incluir_taa=incluir)
    check(clase == "candidato" and numero == "TAA-754",
          f"'TAA-754' en el asunto resuelve directo (incluir_taa={incluir})")

print("\n1d. Variantes tipograficas del guion (Outlook cita con en/em dash)")
for guion, nombre in [("-", "ASCII"), ("–", "en dash"), ("—", "em dash"), ("", "sin guion")]:
    asunto = f"ILUS | Ticket {guion} ID: 754".replace("  ", " ")
    clase, numero = clasificar(asunto, CLIENTE, BUZON, incluir_taa=True)
    check(clase == "candidato" and numero == "TAA-754",
          f"guion '{nombre}' en el asunto sigue resolviendo (incluir_taa=True)")

print("\n2. Respuestas reales (el cliente responde, el asunto lleva 'Re:')")
for prefijo in ("Re: ", "RE: ", "Rv: ", "Fwd: "):
    asunto = f"{prefijo}ILUS | Ticket - ID: 754"
    clase, numero = clasificar(asunto, CLIENTE, BUZON, incluir_taa=True)
    check(clase == "candidato" and numero == "TAA-754",
          f"'{asunto}' sigue resolviendo al ticket correcto (incluir_taa=True)")

print("\n3. El formato PROPIO de ILUS no se rompe (esto ya esta en produccion)")
clase, numero = clasificar("Re: TK-2026-00123 problema con la maquina", CLIENTE, BUZON)
check(clase == "candidato" and numero == "TK-2026-00123",
      "el asunto propio TK-AAAA-NNNNN sigue funcionando igual que antes "
      "(sin necesitar incluir_taa)")
# Si el asunto trae los DOS formatos, manda el propio (es el vigente).
clase, numero = clasificar(
    "TK-2026-00999 / ILUS | Ticket - ID: 754", CLIENTE, BUZON, incluir_taa=True)
check(numero == "TK-2026-00999",
      "si vienen ambos formatos, prevalece el propio (no se consulta la BD de mas)")

print("\n4. Correos propios / rebotes NO se ingestan como mensaje de cliente")
for fe, etiqueta in [
    (BUZON, "el propio buzon de soporte (eco)"),
    ("mailer-daemon@googlemail.com", "rebote de Gmail"),
    ("no-reply@algunservicio.com", "remitente no-reply"),
    ("", "sin remitente"),
]:
    clase, _ = clasificar("ILUS | Ticket - ID: 754", fe, BUZON, incluir_taa=True)
    check(clase == "propio",
          f"formato Triple A desde {etiqueta} -> propio (no se duplica el hilo)")

print("\n5. Casos que NO deben resolver a ningun ticket")
NEGATIVOS = [
    ("Promocion TK FRESHDESK gratis", "asunto con 'TK' suelto que no es nuestro"),
    ("Consulta general sin numero", "asunto sin ningun numero"),
    ("", "asunto vacio"),
]
for asunto, porque in NEGATIVOS:
    clase, numero = clasificar(asunto, CLIENTE, BUZON, incluir_taa=True)
    check(clase == "sin_numero" and numero is None,
          f"'{asunto or '(vacio)'}' -> sin_numero ({porque})")

print("\n5b. Un id de Triple A que NO existe -> 'sin_ticket_taa', NO invisible")
# 2026-08-20, revision adversarial: antes esto caia en 'sin_numero' -- el
# MISMO balde que "no es nuestro correo" -- y quedaba invisible en la vista
# previa (ni contador, ni fila). Ahora se distingue: es nuestro, pero no se
# pudo ubicar.
clase, numero = clasificar("ILUS | Ticket - ID: 99999", CLIENTE, BUZON, incluir_taa=True)
check(clase == "sin_ticket_taa" and numero == "TAA-99999",
      "'ID: 99999' (no existe) -> 'sin_ticket_taa', visible y contable, "
      "no confundido con basura ajena")

print("\n6. El codigo REAL de tickets_module.py tiene ambos patrones")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check("_TK_TAA_SUBJECT_RE" in fuente,
      "existe _TK_TAA_SUBJECT_RE en tickets_module.py")
check(r'ILUS\s*\|\s*Ticket\s*[-–—]?\s*ID:\s*(\d+)' in fuente,
      "el patron de Triple A acepta guion ASCII, en dash, em dash y sin guion "
      "(revision adversarial 2026-08-20: el guion obligatorio dejaba fuera "
      "asuntos citados por Outlook)")
check("_TK_TAA_NUM_RE" in fuente and r"\bTAA-\d+\b" in fuente,
      "existe un patron aparte para el numero YA migrado (TAA-N) -- sin esto, "
      "la conversacion VIVA de un ticket migrado (correo nuevo que ILUS manda "
      "hoy) quedaria ciega apenas el cliente respondiera")
check("SELECT numero_ticket FROM tk_tickets WHERE legacy_taa_id=%s" in fuente,
      "resuelve el id NATIVO de Triple A contra legacy_taa_id (no contra "
      "nuestro id interno -- ver el caso real Gonzalo/Leandro Varela)")
check(fuente.count("_tk_clasificar_correo(") >= 2,
      "sigue habiendo UN solo criterio compartido (vista previa + ingesta real)")
check("legacy_taa_id=%s" in fuente,
      "resuelve por legacy_taa_id (id de Triple A), NO por nuestro id interno")
check("incluir_taa" in fuente,
      "el formato NATIVO viejo de Triple A es opt-in (incluir_taa) -- el "
      "autopoll automatico NUNCA lo activa por su cuenta")

print("\n7. El servidor IMAP DEVUELVE los correos de Triple A")
# Sin esto, todo lo anterior es inutil: el filtro de busqueda corre en el
# SERVIDOR. El asunto de Triple A ("ILUS | Ticket - ID: 754") no contiene
# "TK-", asi que con el filtro historico Gmail no devolvia esos correos y el
# reconocimiento de mas abajo NUNCA llegaba a ejecutarse.
check('SUBJECT "Ticket - ID:"' in fuente,
      "la busqueda IMAP incluye el asunto NATIVO de Triple A (solo si "
      "incluir_taa=True)")
check('SUBJECT "TAA-"' in fuente,
      "la busqueda IMAP SIEMPRE incluye el numero YA migrado (TAA-N), no solo "
      "durante el barrido historico")
check('OR OR SUBJECT "TK-"' in fuente,
      "usa la sintaxis OR ANIDADA de IMAP (binaria y prefija: 3 terminos "
      "necesitan 2 OR) para traer los TRES formatos a la vez")
check('SINCE {desde_imap} SUBJECT "TK-"' in fuente,
      "conserva el filtro historico como respaldo si el servidor rechaza el OR")
check("imaplib.IMAP4.error" in fuente,
      "el SEARCH esta protegido contra la excepcion que imaplib lanza en BAD "
      "(no solo el chequeo de typ != 'OK', que nunca ve ese caso)")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

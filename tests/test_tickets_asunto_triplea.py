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
_TK_TAA_SUBJECT_RE = re.compile(r"ILUS\s*\|\s*Ticket\s*-\s*ID:\s*(\d+)", re.I)

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


def clasificar(subject, from_email, user_email):
    """Copia fiel de _tk_clasificar_correo con la BD simulada."""
    m = _TK_NUM_TICKET_RE.search(subject or "")
    numero = m.group(0).upper() if m else None
    if not numero:
        m_taa = _TK_TAA_SUBJECT_RE.search(subject or "")
        if m_taa:
            try:
                _tid = int(m_taa.group(1))
                _row = mysql_fetchone_falso(
                    "SELECT numero_ticket FROM tk_tickets WHERE legacy_taa_id=%s",
                    (_tid,))
                if _row and _row.get("numero_ticket"):
                    numero = _row["numero_ticket"].upper()
            except Exception:
                pass
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

print("\n1. Asuntos REALES de Triple A (tomados de la bandeja de Daniel)")
# Asuntos textuales de las capturas de la bandeja de soporte.
REALES = [
    ("ILUS | Ticket - ID: 754", "TAA-754"),
    ("ILUS | Ticket - ID: 649", "TAA-649"),
]
for asunto, esperado in REALES:
    clase, numero = clasificar(asunto, CLIENTE, BUZON)
    check(clase == "candidato",
          f"'{asunto}' de un cliente -> candidato (se ingesta al ticket)")
    check(numero == esperado,
          f"'{asunto}' resuelve al ticket real '{esperado}' (obtenido: {numero})")

print("\n1b. NO se confunde el id de Triple A con nuestro id interno")
# Este es el bug que se detecto y corrigio: nuestro ticket id=754 es el
# TAA-567 (otro cliente, otro tema). Si el codigo volviera a resolver por id,
# este check falla y avisa antes de que un cliente vea el error.
_, numero_754 = clasificar("ILUS | Ticket - ID: 754", CLIENTE, BUZON)
check(numero_754 == "TAA-754",
      "'ID: 754' resuelve por legacy_taa_id al TAA-754 (el ticket correcto)")
check(numero_754 != "TAA-567",
      "'ID: 754' NO cae en el TAA-567, que es el que tiene id=754 en nuestra "
      "tabla y pertenece a OTRO cliente (Leandro Varela)")

print("\n2. Respuestas reales (el cliente responde, el asunto lleva 'Re:')")
for prefijo in ("Re: ", "RE: ", "Rv: ", "Fwd: "):
    asunto = f"{prefijo}ILUS | Ticket - ID: 754"
    clase, numero = clasificar(asunto, CLIENTE, BUZON)
    check(clase == "candidato" and numero == "TAA-754",
          f"'{asunto}' sigue resolviendo al ticket correcto")

print("\n3. El formato PROPIO de ILUS no se rompe (esto ya esta en produccion)")
clase, numero = clasificar("Re: TK-2026-00123 problema con la maquina", CLIENTE, BUZON)
check(clase == "candidato" and numero == "TK-2026-00123",
      "el asunto propio TK-AAAA-NNNNN sigue funcionando igual que antes")
# Si el asunto trae los DOS formatos, manda el propio (es el vigente).
clase, numero = clasificar("TK-2026-00999 / ILUS | Ticket - ID: 754", CLIENTE, BUZON)
check(numero == "TK-2026-00999",
      "si vienen ambos formatos, prevalece el propio (no se consulta la BD de mas)")

print("\n4. Correos propios / rebotes NO se ingestan como mensaje de cliente")
for fe, etiqueta in [
    (BUZON, "el propio buzon de soporte (eco)"),
    ("mailer-daemon@googlemail.com", "rebote de Gmail"),
    ("no-reply@algunservicio.com", "remitente no-reply"),
    ("", "sin remitente"),
]:
    clase, _ = clasificar("ILUS | Ticket - ID: 754", fe, BUZON)
    check(clase == "propio",
          f"formato Triple A desde {etiqueta} -> propio (no se duplica el hilo)")

print("\n5. Casos que NO deben resolver a ningun ticket")
NEGATIVOS = [
    ("Promocion TK FRESHDESK gratis", "asunto con 'TK' suelto que no es nuestro"),
    ("Consulta general sin numero", "asunto sin ningun numero"),
    ("ILUS | Ticket - ID: 99999", "ID de Triple A que NO existe en la base"),
    ("", "asunto vacio"),
]
for asunto, porque in NEGATIVOS:
    clase, numero = clasificar(asunto, CLIENTE, BUZON)
    check(clase == "sin_numero" and numero is None,
          f"'{asunto or '(vacio)'}' -> sin_numero ({porque})")

print("\n6. El codigo REAL de tickets_module.py tiene ambos patrones")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check("_TK_TAA_SUBJECT_RE" in fuente,
      "existe _TK_TAA_SUBJECT_RE en tickets_module.py")
check('r"ILUS\\s*\\|\\s*Ticket\\s*-\\s*ID:\\s*(\\d+)"' in fuente,
      "el patron de Triple A del codigo real coincide con el probado aca")
check("SELECT numero_ticket FROM tk_tickets WHERE id=%s" in fuente,
      "resuelve el id de Triple A contra nuestra propia tabla tk_tickets")
check(fuente.count("_tk_clasificar_correo(") >= 2,
      "sigue habiendo UN solo criterio compartido (vista previa + ingesta real)")
check("legacy_taa_id=%s" in fuente,
      "resuelve por legacy_taa_id (id de Triple A), NO por nuestro id interno")

print("\n7. El servidor IMAP DEVUELVE los correos de Triple A")
# Sin esto, todo lo anterior es inutil: el filtro de busqueda corre en el
# SERVIDOR. El asunto de Triple A ("ILUS | Ticket - ID: 754") no contiene
# "TK-", asi que con el filtro historico Gmail no devolvia esos correos y el
# reconocimiento de mas abajo NUNCA llegaba a ejecutarse.
check('SUBJECT "Ticket - ID:"' in fuente,
      "la busqueda IMAP incluye el asunto de Triple A")
check('OR SUBJECT "TK-" SUBJECT "Ticket - ID:"' in fuente,
      "usa la sintaxis OR de IMAP (prefija) para traer AMBOS formatos")
check('SINCE {desde_imap} SUBJECT "TK-"' in fuente,
      "conserva el filtro historico como respaldo si el servidor rechaza el OR")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

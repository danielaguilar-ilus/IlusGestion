"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Respaldo de correos SALIENTES (carpeta Enviados/Sent del mismo buzon) --
2026-08-20.

PEDIDO DE DANIEL (voz a texto, textual): "ademas de traer en la bandeja de
salida o enviados debemos tener ese respaldo en el sistema nuestro". Quiere
que el sistema TAMBIEN lea la carpeta de Enviados/Sent del mismo correo, no
solo INBOX, para que quede un respaldo completo de lo que salio -- sea que
el staff respondio con el boton propio de ILUS, sea que respondio directo
desde Gmail/el sistema viejo de Triple A.

EL HUECO REAL que esto cubre: tk_api_responder_cliente YA registra su propio
envio en tk_mensajes al momento de mandarlo (_tk_log, es_interno=False) --
cuando el staff responde DESDE el boton de ILUS, no hay hueco. El hueco es
cuando alguien responde a un cliente POR FUERA de ILUS: directo desde el
webmail de Gmail, o desde el sistema de Triple A si este manda por el mismo
SMTP durante la marcha blanca. Esos correos NUNCA tocan _tk_log, asi que hoy
son invisibles en el hilo del ticket aunque el cliente SI recibio la
respuesta.

DOS DECISIONES DE DISEÑO QUE ESTE TEST VERIFICA A FONDO:

 1. LA CARPETA DE ENVIADOS NO SE ADIVINA POR NOMBRE. "[Gmail]/Sent Mail"
    solo existe si la cuenta esta en ingles -- en español es "[Gmail]/Correo
    enviado", y otros proveedores usan otro esquema todavia. Se usa la
    extension IMAP SPECIAL-USE (RFC 6154): buscar la carpeta cuyos FLAGS de
    M.list() incluyan "\\Sent", sin importar el nombre ni el idioma.

 2. DEDUP HEURISTICO (no solo por Message-ID). _send_ilus_email (~26
    llamadores en TODO el proyecto) NO se toca esta noche -- el radio de
    impacto es demasiado grande. Sin capturar el Message-ID en el momento
    del envio, la unica forma de saber si un correo de Enviados es el MISMO
    que tk_api_responder_cliente ya registro es comparar contenido + fecha
    dentro de una ventana corta. Deliberadamente conservador: prefiere
    saltarse algun mensaje raro (falso positivo) antes que arriesgar
    duplicar el hilo de un cliente real.

QUE VERIFICA:
 1. _tk_imap_carpeta_especial encuentra la carpeta por el flag \\Sent de
    M.list(), en ingles, en español, con mayusculas/minusculas mezcladas, y
    degrada con gracia (None, no excepcion) si no hay SPECIAL-USE o si
    M.list() falla.
 2. _tk_ya_registrado_como_respuesta (dedup heuristico) REALMENTE descarta
    un mensaje que ya esta en tk_mensajes (misma ventana de tiempo, mismo
    contenido) y REALMENTE deja pasar uno que no coincide con nada.
 3. El barrido completo (_tk_leer_correo_enviado) contra un buzon Enviados
    de mentira: captura lo nuevo, no duplica por Message-ID, no duplica por
    el heuristico de contenido, y un correo sobre un ticket que NO EXISTE
    se cuenta aparte y NO crea un ticket (a diferencia del barrido de
    Inbox, que SI auto-crea para IDs nuevos de Triple A).
 4. dry_run=True no escribe absolutamente nada (ni tk_mensajes, ni
    tk_mail_ingeridos).
 5. Por FUENTE/AST: _tk_leer_correo_enviado nunca llama
    _tk_reabrir_si_cerrado, nunca toca notif_pausada, y el camino de
    "ticket inexistente" termina siempre en continue (nunca cae a un
    camino de insercion).
 6. El código REAL tiene la carpeta enganchada en el autopoll, el cron de
    Cloud Scheduler y el endpoint de recuperación -- los tres con manejo de
    errores propio para no tumbar el resto del barrido si esto falla.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_correo_enviado.py
"""

import ast
import email as _email_mod
import re
import sys
import time
import types
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import format_datetime, parseaddr, parsedate_to_datetime

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# ══════════════════════════════════════════════════════════════════
# Aislar UNA función/asignación del archivo real por INDENTACIÓN, sin
# ast.parse() del archivo COMPLETO -- en esta máquina (Windows, Python
# 3.14) eso puede dar MemoryError de forma reproducible por el tamaño del
# archivo (~90k líneas), no por un error real. Mismo patrón que ya usan
# test_tickets_autocrear_desde_triplea.py y el resto de la batería.
# ══════════════════════════════════════════════════════════════════
with open("tickets_module.py", encoding="utf-8") as _f:
    _LINEAS = _f.readlines()


def _aislar_funcion(nombre_def):
    """Devuelve el texto de UNA función (con su firma), normalizado a
    indentación cero, buscando 'def <nombre>(' y cortando en la próxima
    línea no vacía con indentación <= la de la firma."""
    ini = indent_def = None
    for i, l in enumerate(_LINEAS):
        s = l.lstrip()
        if s.startswith(nombre_def):
            ini = i
            indent_def = len(l) - len(s)
            break
    if ini is None:
        raise AssertionError(f"no se encontró '{nombre_def}' en tickets_module.py")
    fin = len(_LINEAS)
    for i in range(ini + 1, len(_LINEAS)):
        l = _LINEAS[i]
        if not l.strip():
            continue
        if (len(l) - len(l.lstrip())) <= indent_def:
            fin = i
            break
    return "".join(l[indent_def:] if l.strip() else l for l in _LINEAS[ini:fin])


def _aislar_asignacion(nombre):
    """Devuelve el texto de UNA asignación de módulo/closure (constante o
    regex compilado), tolerando que se extienda en varias líneas dentro de
    paréntesis (ej. _TK_IMAP_LIST_RE = re.compile(...))."""
    for i, l in enumerate(_LINEAS):
        ls = l.strip()
        if ls.startswith(nombre + " =") or ls.startswith(nombre + "="):
            indent = len(l) - len(l.lstrip())
            texto = l[indent:]
            j = i
            while texto.count("(") > texto.count(")"):
                j += 1
                texto += _LINEAS[j]
            return texto
    raise AssertionError(f"no se encontró la asignación '{nombre}' en tickets_module.py")


BUZON = "daniel.aguilar@sphs.cl"

with open("tickets_module.py", encoding="utf-8") as _f2:
    FUENTE = _f2.read()


# ══════════════════════════════════════════════════════════════════════
#  1. _tk_imap_carpeta_especial -- SPECIAL-USE, NUNCA por nombre
# ══════════════════════════════════════════════════════════════════════
print("\n1. La carpeta de Enviados se encuentra por el flag \\Sent (RFC 6154 "
      "SPECIAL-USE), NUNCA adivinando el nombre")

_NS_CARPETA = {"re": re}
exec(_aislar_asignacion("_TK_IMAP_LIST_RE"), _NS_CARPETA)
exec(_aislar_funcion("def _tk_imap_carpeta_especial("), _NS_CARPETA)
_carpeta_especial = _NS_CARPETA["_tk_imap_carpeta_especial"]


class FakeMList:
    def __init__(self, entradas, explota=False):
        self.entradas = entradas
        self.explota = explota

    def list(self):
        if self.explota:
            raise OSError("conexión perdida a mitad del LIST")
        return ("OK", self.entradas)


check(_carpeta_especial(FakeMList([
    b'(\\HasNoChildren) "/" "INBOX"',
    b'(\\HasNoChildren \\Sent) "/" "[Gmail]/Sent Mail"',
    b'(\\HasNoChildren \\Trash) "/" "[Gmail]/Trash"',
]), "\\Sent") == "[Gmail]/Sent Mail",
      "cuenta en inglés: encuentra '[Gmail]/Sent Mail' por el flag \\Sent")

check(_carpeta_especial(FakeMList([
    b'(\\HasNoChildren) "/" "INBOX"',
    b'(\\HasNoChildren \\Sent) "/" "[Gmail]/Correo enviado"',
    b'(\\HasNoChildren \\Papelera) "/" "[Gmail]/Papelera"',
]), "\\Sent") == "[Gmail]/Correo enviado",
      "cuenta en ESPAÑOL: encuentra '[Gmail]/Correo enviado' -- el nombre "
      "cambia con el idioma, el flag no. Si el código hubiera hardcodeado "
      "'[Gmail]/Sent Mail' esto fallaría en cualquier cuenta no-inglesa")

check(_carpeta_especial(FakeMList([
    b'(\\HasNoChildren \\SENT) "/" "Elementos enviados"',
]), "\\Sent") == "Elementos enviados",
      "el flag se compara case-insensitive (\\SENT en mayúsculas también "
      "matchea) -- proveedor tipo Office365/Zoho con otra convención")

check(_carpeta_especial(FakeMList([
    b'(\\HasNoChildren) "/" "INBOX"',
    b'(\\HasNoChildren) "/" "Sent Items"',
]), "\\Sent") is None,
      "sin ningún flag \\Sent anunciado (servidor sin SPECIAL-USE), "
      "devuelve None -- NO adivina 'Sent Items' solo porque el nombre "
      "suene parecido")

check(_carpeta_especial(FakeMList([], explota=True), "\\Sent") is None,
      "si M.list() explota (falla de red), degrada a None -- no propaga la "
      "excepción (el llamador debe poder seguir sin romper el resto del "
      "barrido de correo)")

check(_carpeta_especial(FakeMList([b"basura sin formato de LIST"]), "\\Sent") is None,
      "una línea de LIST con formato inesperado no revienta el parseo, "
      "simplemente no matchea")


# ══════════════════════════════════════════════════════════════════════
#  2. Dedup HEURÍSTICO -- ¿ya lo registró tk_api_responder_cliente?
# ══════════════════════════════════════════════════════════════════════
print("\n2. El dedup heurístico REALMENTE descarta lo que ya está en "
      "tk_mensajes, y REALMENTE deja pasar lo que no coincide con nada")

_NS_DEDUP = {"re": re, "timedelta": timedelta, "_html_mod": __import__("html"),
             "print": lambda *a, **k: None}
exec(_aislar_asignacion("_TK_DEDUP_HEUR_VENTANA_MIN"), _NS_DEDUP)
exec(_aislar_funcion("def _tk_normalizar_para_dedup("), _NS_DEDUP)


class BancoMensajes:
    """mysql_fetchall de mentira, solo para la consulta del dedup heurístico."""

    def __init__(self, filas):
        self.filas = filas   # [{"ticket_id":, "created_at":, "contenido":}]

    def fetchall(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM tk_mensajes WHERE ticket_id" not in s:
            return []
        tid, desde, hasta = params
        return [{"contenido": f["contenido"]} for f in self.filas
                if f["ticket_id"] == tid and desde <= f["created_at"] <= hasta]


_banco_dedup = BancoMensajes([
    {"ticket_id": 42, "created_at": datetime(2026, 8, 20, 15, 1, 10),
     "contenido": '<div>Hola Ana, el tecnico va manana a las 10.</div>'},
])
_NS_DEDUP["mysql_fetchall"] = _banco_dedup.fetchall
exec(_aislar_funcion("def _tk_ya_registrado_como_respuesta("), _NS_DEDUP)
_ya_registrado = _NS_DEDUP["_tk_ya_registrado_como_respuesta"]

check(_ya_registrado(42, "Hola Ana, el tecnico va manana a las 10.",
                     datetime(2026, 8, 20, 15, 0, 0)) is True,
      "mismo ticket, mismo texto (sin el wrapper HTML) y dentro de la "
      "ventana de 3 minutos -> SÍ está duplicado, no hay que volver a "
      "insertarlo")

check(_ya_registrado(42, "Un mensaje completamente distinto sobre otra falla.",
                     datetime(2026, 8, 20, 15, 0, 30)) is False,
      "mismo ticket y misma ventana de tiempo, pero CONTENIDO distinto -> "
      "NO está duplicado, se deja pasar (conservador: prefiere un posible "
      "duplicado raro antes que perder un correo real)")

check(_ya_registrado(42, "Hola Ana, el tecnico va manana a las 10.",
                     datetime(2026, 8, 20, 15, 20, 0)) is False,
      "mismo texto pero FUERA de la ventana de tiempo (+19 min) -> no se "
      "considera el mismo envío")

check(_ya_registrado(99, "Hola Ana, el tecnico va manana a las 10.",
                     datetime(2026, 8, 20, 15, 1, 0)) is False,
      "mismo texto y misma hora pero OTRO ticket -> nunca cruza tickets")

check(_ya_registrado(42, "cualquier texto", None) is False,
      "sin fecha real del correo (Date ilegible) no hay ventana confiable: "
      "se asume NO duplicado -- mejor un duplicado raro que perder un "
      "correo real por falta de fecha")


# ══════════════════════════════════════════════════════════════════════
#  3. Barrido completo (_tk_leer_correo_enviado) contra un buzón de mentira
# ══════════════════════════════════════════════════════════════════════
print("\n3. El barrido completo captura lo nuevo, no duplica (ni por "
      "Message-ID ni por contenido) y NO auto-crea tickets")


class BuzonProhibido(AssertionError):
    """Se lanza si el código intenta MODIFICAR la carpeta Enviados real."""


class FakeIMAPEnviados:
    def __init__(self, mensajes):
        self.mensajes = mensajes
        self.select_readonly = None
        self.select_mailbox = None
        self.fetch_specs = []
        self.search_criterio = None
        self.logout_llamado = False

    def login(self, user, pwd):
        return ("OK", [b"LOGIN completed"])

    def select(self, mailbox, readonly=False):
        self.select_mailbox = mailbox
        self.select_readonly = readonly
        if not readonly:
            raise BuzonProhibido("select() sin readonly=True")
        return ("OK", [b"1"])

    def search(self, charset, criterio):
        self.search_criterio = criterio
        ids = b" ".join(str(i + 1).encode() for i in range(len(self.mensajes)))
        return ("OK", [ids])

    def fetch(self, mid, spec):
        self.fetch_specs.append(spec)
        if "BODY.PEEK" not in spec:
            raise BuzonProhibido(f"fetch sin BODY.PEEK: {spec}")
        raw = self.mensajes[int(mid) - 1]
        if "HEADER" in spec:
            corte = raw.find(b"\n\n")
            raw = raw[:corte + 1] if corte > 0 else raw
        return ("OK", [(b"1 (RFC822 {})", raw)])

    def logout(self):
        self.logout_llamado = True
        return ("BYE", [b"ok"])

    def store(self, *a, **k):
        raise BuzonProhibido("store() -- marcaría el correo como leído")

    def copy(self, *a, **k):
        raise BuzonProhibido("copy()")

    def move(self, *a, **k):
        raise BuzonProhibido("move()")

    def expunge(self, *a, **k):
        raise BuzonProhibido("expunge()")


def _construir_mail(asunto, de, texto, message_id, cuando):
    m = EmailMessage()
    m["Subject"] = asunto
    m["From"] = de
    m["To"] = "cliente@empresa.cl"
    m["Message-ID"] = message_id
    m["Date"] = format_datetime(cuando)
    m.set_content(texto)
    return m.as_bytes()


class Banco:
    def __init__(self, tickets, ya_ingeridos=(), mensajes_previos=()):
        self.tickets = dict(tickets)
        self.ya_ingeridos = set(ya_ingeridos)
        self.mensajes_previos = list(mensajes_previos)
        self.ejecutados = []

    def fetchone(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM tk_tickets WHERE numero_ticket" in s:
            return self.tickets.get(params[0])
        if "FROM tk_mail_ingeridos WHERE message_id" in s:
            return {"message_id": params[0]} if params[0] in self.ya_ingeridos else None
        return None

    def fetchall(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM tk_mensajes WHERE ticket_id" in s:
            tid, desde, hasta = params
            return [{"contenido": f["contenido"]} for f in self.mensajes_previos
                    if f["ticket_id"] == tid and desde <= f["created_at"] <= hasta]
        return []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.ejecutados.append((s, params))
        if "INSERT IGNORE INTO tk_mail_ingeridos" in s:
            self.ya_ingeridos.add(params[0])
        return 1

    def escrituras(self):
        return [s for s, _ in self.ejecutados]

    def cuenta(self, fragmento):
        return sum(1 for s in self.escrituras() if fragmento in s)


def _stub_extraer_cuerpo(msg):
    """Stub mínimo (solo texto plano) -- el recorte de cola citada de
    _tk_extraer_cuerpo_mail ya está cubierto en otro lado; acá solo hace
    falta CONTENIDO real para ejercitar captura + dedup heurístico."""
    for part in msg.walk():
        if part.get_content_type() == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                return payload.decode(part.get_content_charset() or "utf-8", "replace")
    return ""


def _correr_barrido_enviado(mensajes, tickets, ya_ingeridos=(),
                            mensajes_previos=(), **kw):
    banco = Banco(tickets, ya_ingeridos, mensajes_previos)
    imap = FakeIMAPEnviados(mensajes)
    efectos = {"log": []}

    def _tk_log(ticket_id, tipo, contenido, **k):
        efectos["log"].append({"ticket_id": ticket_id, "tipo": tipo,
                               "contenido": contenido, **k})
        return 900 + len(efectos["log"])

    ns = {
        "re": re, "time": time,
        "datetime": datetime, "timezone": timezone, "timedelta": timedelta,
        "_email_mod": _email_mod, "make_header": make_header,
        "decode_header": decode_header, "parseaddr": parseaddr,
        "parsedate_to_datetime": parsedate_to_datetime,
        "print": lambda *a, **k: None,
        "imaplib": types.SimpleNamespace(
            IMAP4_SSL=lambda host, port, timeout=None: imap),
        "_tk_imap_creds": lambda: (BUZON, "clave-app"),
        "_tk_imap_host": lambda: "imap.gmail.com",
        # Se stubea (no se reusa la real): la mecánica SPECIAL-USE ya se
        # probó a fondo en la sección 1, aislada. Acá lo que importa es el
        # comportamiento del barrido UNA VEZ que la carpeta se encontró.
        "_tk_imap_carpeta_especial": lambda M, flag: "[Gmail]/Sent Mail",
        "_TK_SENT_FLAG": "\\Sent",
        "mysql_fetchone": banco.fetchone,
        "mysql_fetchall": banco.fetchall,
        "mysql_execute": banco.execute,
        "_tk_log": _tk_log,
        "_tk_extraer_cuerpo_mail": _stub_extraer_cuerpo,
        "_html_mod": __import__("html"),
        "_fmt_dt": lambda v, only_date=False: (v.strftime("%d/%m/%Y %H:%M")
                                               if hasattr(v, "strftime") else ""),
    }
    exec(_aislar_asignacion("_TK_NUM_TICKET_RE"), ns)
    exec(_aislar_asignacion("_TK_TAA_NUM_RE"), ns)
    exec(_aislar_asignacion("_TK_DEDUP_HEUR_VENTANA_MIN"), ns)
    exec(_aislar_funcion("def _tk_normalizar_para_dedup("), ns)
    exec(_aislar_funcion("def _tk_ya_registrado_como_respuesta("), ns)
    exec(_aislar_funcion("def _tk_leer_correo_enviado("), ns)
    resumen = ns["_tk_leer_correo_enviado"](**kw)
    return resumen, banco, imap, efectos


TICKET_42 = {"id": 42, "numero_ticket": "TK-2026-00042"}
FECHA_BASE = datetime(2026, 8, 20, 15, 0, 0, tzinfo=timezone.utc)

MSG_NUEVO = _construir_mail(
    "Re: [TK-2026-00042] Repuesto", BUZON,
    "Ya despachamos el repuesto nuevo, llega mañana.",
    "<nuevo-sent-1@mail.gmail.com>", FECHA_BASE)
MSG_DUPLICADO_MESSAGE_ID = _construir_mail(
    "Re: [TK-2026-00042] Ya en el hilo", BUZON,
    "Este correo ya quedó ingerido antes (ej. autoenvío visto también en Inbox).",
    "<ya-ingerido-1@mail.gmail.com>", FECHA_BASE)
MSG_SIN_TICKET = _construir_mail(
    "Re: [TK-2026-99999] No existe", BUZON,
    "Correspondencia vieja de un ticket que no existe en ILUS.",
    "<huerfano-sent-1@mail.gmail.com>", FECHA_BASE)
MSG_YA_REGISTRADO = _construir_mail(
    "Re: [TK-2026-00042] Confirmacion visita", BUZON,
    "Hola Ana, el tecnico va manana a las 10.",
    "<respondido-boton-ilus-1@mail.gmail.com>", FECHA_BASE + timedelta(minutes=1, seconds=10))
MSG_FORMATO_TRIPLEA_NATIVO = _construir_mail(
    "ILUS | Ticket - ID: 754", BUZON,
    "Formato nativo viejo de Triple A -- no aplica para Enviados.",
    "<nativo-triplea-1@mail.gmail.com>", FECHA_BASE)

TODOS = [MSG_NUEVO, MSG_DUPLICADO_MESSAGE_ID, MSG_SIN_TICKET,
         MSG_YA_REGISTRADO, MSG_FORMATO_TRIPLEA_NATIVO]
YA_INGERIDOS = ["<ya-ingerido-1@mail.gmail.com>"]
MENSAJES_PREVIOS = [
    {"ticket_id": 42, "created_at": FECHA_BASE.replace(tzinfo=None) + timedelta(minutes=1),
     "contenido": "Hola Ana, el tecnico va manana a las 10."},
]
TICKETS = {"TK-2026-00042": TICKET_42}


print("\n3a. Vista previa (dry_run=True): clasifica bien y NO escribe nada")
resumen_dry, banco_dry, imap_dry, ef_dry = _correr_barrido_enviado(
    TODOS, TICKETS, YA_INGERIDOS, MENSAJES_PREVIOS,
    dias=20, max_correos=50, dry_run=True, detalle=True)
check(resumen_dry["ok"], "el barrido corre sin error contra el buzón de mentira")
check(resumen_dry["capturados"] == 1, "solo MSG_NUEVO se va a capturar")
check(resumen_dry["duplicados"] == 1, "MSG_DUPLICADO_MESSAGE_ID ya está por Message-ID")
check(resumen_dry["omitidos_sin_ticket"] == 1, "MSG_SIN_TICKET no resuelve a ningún ticket")
check(resumen_dry["ya_registrados"] == 1,
      "MSG_YA_REGISTRADO lo descarta el dedup heurístico (tk_api_responder_cliente ya lo guardó)")
check(banco_dry.ejecutados == [], "dry_run=True no escribe NADA en la base")
check(ef_dry["log"] == [], "dry_run=True no llama _tk_log ni una vez")
check("TK-2026-99999" not in banco_dry.tickets,
      "dry_run tampoco crea el ticket inexistente")

print("\n3b. Ingesta real (dry_run=False): captura solo lo que corresponde")
resumen, banco, imap, ef = _correr_barrido_enviado(
    TODOS, TICKETS, YA_INGERIDOS, MENSAJES_PREVIOS,
    dias=20, max_correos=50, dry_run=False, detalle=True)
check(resumen["capturados"] == 1, "se captura exactamente 1 mensaje nuevo")
check(len(ef["log"]) == 1, "_tk_log se llamó exactamente 1 vez")
_msg_capturado = ef["log"][0]
check(_msg_capturado["ticket_id"] == 42, "el mensaje capturado va al ticket correcto (id=42)")
check(_msg_capturado["tipo"] == "mensaje",
      "tipo='mensaje' -- NO 'client_message' (esto es una respuesta NUESTRA, no del cliente)")
check(_msg_capturado.get("es_interno") is False,
      "es_interno=False -- se renderiza como respuesta saliente normal en la ficha, "
      "sin tocar el frontend (ver static/tickets_ficha.js)")
check("despachamos el repuesto" in _msg_capturado["contenido"],
      "el contenido guardado es el del correo realmente capturado")
check(banco.cuenta("INSERT IGNORE INTO tk_mail_ingeridos") == 1,
      "se marca en tk_mail_ingeridos SOLO el que se capturó de verdad")
check("TK-2026-99999" not in banco.tickets,
      "el ticket inexistente sigue sin existir -- Enviados NUNCA auto-crea "
      "(a diferencia del barrido de Inbox con IDs nuevos de Triple A)")
check(all("INSERT INTO tk_tickets" not in s for s in banco.escrituras()),
      "en ningún momento se ejecuta un INSERT a tk_tickets")

print("\n3c. Segunda pasada: idempotencia real (nada se duplica)")
resumen2, banco2, _, ef2 = _correr_barrido_enviado(
    TODOS, TICKETS, list(banco.ya_ingeridos), MENSAJES_PREVIOS,
    dias=20, max_correos=50, dry_run=False)
check(resumen2["capturados"] == 0, "nada nuevo que capturar -- ya quedó todo marcado")
check(ef2["log"] == [], "segunda pasada no vuelve a llamar _tk_log")

print("\n3d. Reglas innegociables del buzón (misma casilla real de Daniel)")
check(imap.select_readonly is True, "select() de Enviados es SIEMPRE readonly=True")
check(imap.select_mailbox.startswith('"') and imap.select_mailbox.endswith('"'),
      "el nombre de la carpeta se pasa ENTRE COMILLAS a select() -- imaplib NO "
      "cita automático y el nombre real trae espacios/corchetes "
      "('[Gmail]/Sent Mail')")
check(all("BODY.PEEK" in s for s in imap.fetch_specs), "todo fetch usa BODY.PEEK")
check(imap.logout_llamado, "la sesión se cierra siempre al terminar")


# ══════════════════════════════════════════════════════════════════════
#  4. Por FUENTE/AST: lo que _tk_leer_correo_enviado NUNCA hace
# ══════════════════════════════════════════════════════════════════════
print("\n4. Por fuente/AST: _tk_leer_correo_enviado nunca reabre tickets, "
      "nunca toca notif_pausada, y el camino sin-ticket siempre corta")

_SLICE_ENVIADO = _aislar_funcion("def _tk_leer_correo_enviado(")
_arbol_enviado = ast.parse(_SLICE_ENVIADO)

# El docstring de la función EXPLICA en prosa por qué no se llama
# _tk_reabrir_si_cerrado ni se toca notif_pausada -- así que ambas palabras
# SI aparecen en el texto crudo de la función, pero solo como comentario.
# Para que el check sea honesto (verificar el CÓDIGO, no la prosa que lo
# explica), se aísla el cuerpo SIN el docstring vía AST y se busca ahí --
# mismo patrón que ya usa test_tickets_mail_recuperacion.py para un caso
# equivalente (TestEsquemaSobreviveASkipMigrations).
_nodo_funcion_enviado = None
for _nodo in ast.walk(_arbol_enviado):
    if isinstance(_nodo, ast.FunctionDef) and _nodo.name == "_tk_leer_correo_enviado":
        _nodo_funcion_enviado = _nodo
        break
check(_nodo_funcion_enviado is not None,
      "se pudo parsear _tk_leer_correo_enviado como un único FunctionDef")
_cuerpo_sin_doc = (
    _nodo_funcion_enviado.body[1:]
    if (_nodo_funcion_enviado.body
        and isinstance(_nodo_funcion_enviado.body[0], ast.Expr)
        and isinstance(_nodo_funcion_enviado.body[0].value, ast.Constant))
    else _nodo_funcion_enviado.body)
_CODIGO_SIN_DOC = "\n".join(ast.unparse(x) for x in _cuerpo_sin_doc)

check("_tk_reabrir_si_cerrado" not in _CODIGO_SIN_DOC,
      "el CÓDIGO (sin el docstring que lo explica) de _tk_leer_correo_enviado "
      "no contiene ninguna referencia a _tk_reabrir_si_cerrado -- una "
      "respuesta SALIENTE no debe reabrir nada (esa es una decisión que el "
      "staff toma directo en la UI)")
check("notif_pausada" not in _CODIGO_SIN_DOC,
      "el CÓDIGO (sin el docstring) de _tk_leer_correo_enviado no contiene "
      "ninguna referencia a notif_pausada -- el respaldo de Enviados no "
      "debe afectar la pausa de notificaciones de los tickets migrados")
check("auto_creados" not in _CODIGO_SIN_DOC and "sistema-correo-triplea" not in _CODIGO_SIN_DOC,
      "no aparece ningún rastro del mecanismo de auto-creación del Inbox "
      "(auto_creados / created_by='sistema-correo-triplea') -- Enviados "
      "NUNCA auto-crea tickets")
check("ILUS | Ticket - ID" not in _CODIGO_SIN_DOC and "_TK_TAA_SUBJECT_RE" not in _CODIGO_SIN_DOC,
      "no se reconoce el formato NATIVO viejo de Triple A -- ese es el "
      "asunto que TRIPLE A le pone a SUS correos, no a los que salen de "
      "nuestra cuenta")

# El punto más importante de esta sección, verificado por ESTRUCTURA (AST)
# y no por texto plano: el bloque `if not ticket:` (ticket inexistente)
# tiene que terminar SIEMPRE en un continue -- nunca puede caer a un
# camino que inserte o cree algo. Mismo patrón que ya usa
# test_tickets_autocrear_desde_triplea.py para el bloque hermano del Inbox.


def _es_comparacion_not_ticket(nodo_test):
    return (isinstance(nodo_test, ast.UnaryOp) and isinstance(nodo_test.op, ast.Not)
            and isinstance(nodo_test.operand, ast.Name)
            and nodo_test.operand.id == "ticket")


_nodo_sin_ticket = None
for _nodo in ast.walk(_arbol_enviado):
    if isinstance(_nodo, ast.If) and _es_comparacion_not_ticket(_nodo.test):
        _nodo_sin_ticket = _nodo
        break

check(_nodo_sin_ticket is not None,
      "existe el bloque `if not ticket:` en el código real de "
      "_tk_leer_correo_enviado")
if _nodo_sin_ticket is not None:
    _ultima = _nodo_sin_ticket.body[-1]
    check(isinstance(_ultima, ast.Continue),
          "la ÚLTIMA instrucción del bloque `if not ticket:` es un continue "
          "-- un ticket inexistente NUNCA cae a un camino de inserción o "
          "creación (a diferencia del Inbox, donde 'sin_ticket_taa' SÍ "
          "puede seguir de largo)")
    _codigo_bloque = ast.unparse(_nodo_sin_ticket)
    check("tk_tickets" not in _codigo_bloque,
          "dentro del bloque de ticket inexistente no se toca tk_tickets "
          "para nada -- ni lectura de más, ni escritura")

# El bloque hermano `if dry_run:` (justo antes de escribir de verdad)
# también tiene que terminar en continue -- si no, dry_run=True escribiría.
_nodo_dry_run = None
for _nodo in ast.walk(_arbol_enviado):
    if (isinstance(_nodo, ast.If) and isinstance(_nodo.test, ast.Name)
            and _nodo.test.id == "dry_run"):
        _nodo_dry_run = _nodo
        break
check(_nodo_dry_run is not None, "existe el bloque `if dry_run:` en el código real")
if _nodo_dry_run is not None:
    check(isinstance(_nodo_dry_run.body[-1], ast.Continue),
          "el bloque `if dry_run:` termina en continue -- nunca llega a "
          "_tk_log ni a mysql_execute cuando es solo vista previa")
    _codigo_dry = ast.unparse(_nodo_dry_run)
    check("_tk_log" not in _codigo_dry and "mysql_execute" not in _codigo_dry,
          "dentro del bloque `if dry_run:` no hay ninguna llamada a "
          "_tk_log ni a mysql_execute")


# ══════════════════════════════════════════════════════════════════════
#  5. El código REAL tiene el respaldo enganchado en los 3 disparadores
#     (autopoll, cron de Cloud Scheduler, endpoint de recuperación)
# ══════════════════════════════════════════════════════════════════════
print("\n5. _tk_leer_correo_enviado queda enganchado en autopoll, cron y "
      "el endpoint de recuperación -- cada uno con su propio manejo de "
      "errores para no tumbar el resto del barrido")

check(FUENTE.count("_tk_leer_correo_enviado(") >= 4,
      "la función se define y se llama desde AL MENOS los 3 disparadores "
      "(autopoll, cron, recuperar) además de su propia definición")

_SLICE_AUTOPOLL = _aislar_funcion("def _tk_autopoll_correo(")
check("_tk_leer_correo_enviado" in _SLICE_AUTOPOLL,
      "_tk_autopoll_correo llama a _tk_leer_correo_enviado")
check("except Exception as _e_env:" in _SLICE_AUTOPOLL,
      "el autopoll envuelve la llamada en su propio try/except -- un fallo "
      "leyendo Enviados no debe tumbar el autopoll de Inbox (lo crítico: "
      "correos de CLIENTES)")

_SLICE_CRON = _aislar_funcion("def tk_cron_leer_correo(")
check("_tk_leer_correo_enviado" in _SLICE_CRON,
      "tk_cron_leer_correo (Cloud Scheduler) llama a _tk_leer_correo_enviado")
check('resumen["enviados"]' in _SLICE_CRON,
      "el resultado de Enviados queda en su propia clave del resumen del "
      "cron, no mezclado con los contadores de Inbox")
check("except Exception as _e_env:" in _SLICE_CRON,
      "el cron también envuelve la llamada en su propio try/except")

_SLICE_RECUPERAR = _aislar_funcion("def tk_api_mail_recuperar(")
check("_tk_leer_correo_enviado" in _SLICE_RECUPERAR,
      "tk_api_mail_recuperar (vista previa + ingesta real, superadmin) "
      "también llama a _tk_leer_correo_enviado")
check('resumen["enviados"] = _tk_leer_correo_enviado(' in _SLICE_RECUPERAR,
      "el resultado de Enviados se suma al resumen bajo su PROPIA clave "
      "('enviados') -- no se mezcla con los contadores del Inbox, así "
      "queda claro qué carpeta trajo qué")
_idx_llamada_enviado = _SLICE_RECUPERAR.index(
    'resumen["enviados"] = _tk_leer_correo_enviado(')
check("dry_run=dry_run" in _SLICE_RECUPERAR[_idx_llamada_enviado:_idx_llamada_enviado + 300],
      "la llamada REAL a Enviados (no el comentario que la explica arriba) "
      "respeta el MISMO dry_run que pidió quien llama al endpoint -- la "
      "vista previa de Enviados es tan honesta como la de Inbox")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

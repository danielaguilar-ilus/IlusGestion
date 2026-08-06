"""Pruebas del canal de RECEPCIÓN de correos de Tickets.

CONTEXTO — el incidente de los 12 días (24-jul → 5-ago-2026): los correos de
clientes dejaron de convertirse en tickets. Nadie lo notó porque el ENVÍO
seguía funcionando y la app no mostraba NADA sobre la recepción. La falla no
fue técnica, fue INVISIBLE.

Lo que se prueba acá:
  1. La clasificación de cada correo (el criterio que comparten la VISTA
     PREVIA y la INGESTA real — si se separaran, la previa mentiría).
  2. El recorte de la cola citada del hilo, incluida la red de seguridad que
     impide perder el mensaje del cliente cuando el recorte se pasa de listo.
  3. El semáforo del latido de recepción (tk_mail_salud).
  4. El barrido completo contra un buzón IMAP falso: correo nuevo, correo ya
     ingerido, TK- inexistente, correo propio, correo con adjunto — en modo
     vista previa (no escribe nada) y en modo ingesta.
  5. Las reglas innegociables sobre la casilla real de Daniel: BODY.PEEK,
     select(readonly=True), cero comandos que modifiquen el buzón, y CERO
     correo saliente al ingerir (nada de acuses de recibo a clientes reales).

No se importa tickets_module directo: register_tickets_routes necesita una app
Flask y un ctx completo. Se extraen las funciones con `ast` y se ejecutan
aisladas — mismo patrón de tests/test_tickets_imap_host.py.

Correr con:  py -m unittest tests.test_tickets_mail_recuperacion -v
"""
import ast
import email as _email_mod
import io
import re
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import format_datetime, parseaddr, parsedate_to_datetime

SRC = open("tickets_module.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(SRC)


def _fuente_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en tickets_module.py")


def _fuente_asignacion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.Assign):
            for t in nodo.targets:
                if isinstance(t, ast.Name) and t.id == nombre:
                    return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la asignación '{nombre}' en tickets_module.py")


def _ns_base():
    """Namespace mínimo compartido por las funciones extraídas."""
    return {
        "re": re, "io": io, "os": __import__("os"), "time": time,
        "datetime": datetime, "timezone": timezone, "timedelta": timedelta,
        "_email_mod": _email_mod, "make_header": make_header,
        "decode_header": decode_header, "parseaddr": parseaddr,
        "parsedate_to_datetime": parsedate_to_datetime,
        "print": lambda *a, **k: None,   # silencio en los tests
    }


# ══════════════════════════════════════════════════════════════════════
#  1. Clasificación: el criterio ÚNICO de previa + ingesta
# ══════════════════════════════════════════════════════════════════════
_NS_CLASIF = _ns_base()
exec(_fuente_asignacion("_TK_NUM_TICKET_RE"), _NS_CLASIF)
exec(_fuente_funcion("_tk_clasificar_correo"), _NS_CLASIF)
_clasificar = _NS_CLASIF["_tk_clasificar_correo"]

BUZON = "daniel.aguilar@sphs.cl"


class TestClasificacion(unittest.TestCase):
    def test_respuesta_normal_de_cliente(self):
        self.assertEqual(
            ("candidato", "TK-2026-00042"),
            _clasificar("Re: [TK-2026-00042] Falla en la trotadora",
                        "cliente@empresa.cl", BUZON))

    def test_numero_en_minuscula_se_normaliza(self):
        self.assertEqual(
            ("candidato", "TK-2026-00042"),
            _clasificar("re: tk-2026-00042 consulta", "cliente@empresa.cl", BUZON))

    def test_asunto_sin_numero_de_ticket(self):
        # "TK FRESHDESK" y promociones con "TK-" suelto: no son nuestros.
        self.assertEqual(("sin_numero", None),
                         _clasificar("TK FRESHDESK novedades", "x@y.cl", BUZON))

    def test_formato_parecido_pero_invalido_no_pasa(self):
        # TK-2026-123 (3 dígitos) NO es un número nuestro (son 5).
        self.assertEqual(("sin_numero", None),
                         _clasificar("Re: TK-2026-123", "x@y.cl", BUZON))

    def test_correo_propio_no_se_ingiere(self):
        # Nuestro propio envío rebotando en el buzón: si se ingiriera, el
        # hilo del ticket se llenaría de eco de lo que ya mandamos.
        self.assertEqual(("propio", "TK-2026-00042"),
                         _clasificar("[TK-2026-00042] Respuesta", BUZON, BUZON))

    def test_correo_propio_ignora_mayusculas(self):
        self.assertEqual(("propio", "TK-2026-00042"),
                         _clasificar("[TK-2026-00042] x", "Daniel.Aguilar@SPHS.cl", BUZON))

    def test_rebote_mailer_daemon(self):
        self.assertEqual(
            ("propio", "TK-2026-00042"),
            _clasificar("[TK-2026-00042] Undelivered",
                        "mailer-daemon@googlemail.com", BUZON))

    def test_buzon_noreply(self):
        self.assertEqual(("propio", "TK-2026-00042"),
                         _clasificar("[TK-2026-00042] aviso", "no-reply@x.cl", BUZON))

    def test_sin_remitente(self):
        self.assertEqual(("propio", "TK-2026-00042"),
                         _clasificar("[TK-2026-00042] x", "", BUZON))


# ══════════════════════════════════════════════════════════════════════
#  2. Recorte de la cola citada del hilo
# ══════════════════════════════════════════════════════════════════════
_NS_CUERPO = _ns_base()
exec(_fuente_asignacion("_TK_QUOTE_RE"), _NS_CUERPO)
exec(_fuente_funcion("_tk_extraer_cuerpo_mail"), _NS_CUERPO)
_cuerpo = _NS_CUERPO["_tk_extraer_cuerpo_mail"]


def _mail_texto(texto, subtype="plain"):
    m = EmailMessage()
    m["From"] = "cliente@empresa.cl"
    m["Subject"] = "Re: [TK-2026-00042] prueba"
    m.set_content(texto, subtype=subtype)
    return _email_mod.message_from_bytes(m.as_bytes())


class TestCuerpoDelMensaje(unittest.TestCase):
    def test_texto_plano_simple(self):
        self.assertEqual("La máquina sigue con ruido.",
                         _cuerpo(_mail_texto("La máquina sigue con ruido.")))

    def test_corta_la_cola_citada_de_gmail(self):
        crudo = ("Sí, quedó funcionando. Gracias.\n\n"
                 "El mar, 5 ago 2026 a las 10:02, ILUS escribió:\n"
                 "> Estimado cliente, adjuntamos el informe...\n")
        self.assertEqual("Sí, quedó funcionando. Gracias.", _cuerpo(_mail_texto(crudo)))

    def test_corta_la_cola_citada_en_ingles(self):
        crudo = "Confirmed.\n\nOn Wed, Aug 5, 2026 at 10:02 ILUS wrote:\n> hola\n"
        self.assertEqual("Confirmed.", _cuerpo(_mail_texto(crudo)))

    def test_descarta_lineas_citadas_sueltas(self):
        self.assertEqual("Listo.", _cuerpo(_mail_texto("Listo.\n> texto viejo\n")))

    def test_solo_html_se_convierte_a_texto(self):
        m = EmailMessage()
        m["From"] = "cliente@empresa.cl"
        m.set_content("<p>Hola,<br>ya lo revis&oacute;.</p>", subtype="html")
        texto = _cuerpo(_email_mod.message_from_bytes(m.as_bytes()))
        self.assertIn("Hola", texto)
        self.assertIn("revisó", texto)
        self.assertNotIn("<p>", texto)

    # ── La red de seguridad (bug real que esto previene) ──
    def test_no_pierde_el_mensaje_si_arranca_con_un_marcador_de_cita(self):
        """Outlook pone el bloque "De: … Enviado: …" ARRIBA cuando el cliente
        responde sin escribir encima. Con el recorte a secas, el mensaje
        entero desaparecía y el ticket guardaba "(Mensaje sin texto)" — y
        como el dedup por Message-ID lo marcaba ingerido, el contenido real
        del cliente se perdía para siempre y en silencio."""
        crudo = ("De: ILUS <soporte@ilusfitness.com>\n"
                 "Enviado: martes, 5 de agosto de 2026\n"
                 "Asunto: [TK-2026-00042] Su solicitud\n\n"
                 "NO ME LLEGÓ EL REPUESTO, POR FAVOR URGENTE\n")
        salida = _cuerpo(_mail_texto(crudo))
        self.assertTrue(salida.strip(), "el cuerpo no puede quedar vacío")
        self.assertIn("NO ME LLEGÓ EL REPUESTO", salida)

    def test_no_pierde_el_mensaje_si_arranca_con_enviado_desde_mi_iphone(self):
        crudo = "Enviado desde mi iPhone\n\nSigue sin funcionar, ayuda\n"
        salida = _cuerpo(_mail_texto(crudo))
        self.assertIn("Sigue sin funcionar", salida)

    def test_mail_realmente_vacio_devuelve_vacio(self):
        self.assertEqual("", _cuerpo(_mail_texto("   \n\n  \n")))


# ══════════════════════════════════════════════════════════════════════
#  3. Semáforo del latido de recepción
# ══════════════════════════════════════════════════════════════════════
_NS_SALUD = _ns_base()
exec(_fuente_asignacion("_TK_MAIL_DIAS_SILENCIO"), _NS_SALUD)
exec(_fuente_funcion("_tk_salud_evaluar"), _NS_SALUD)
_evaluar = _NS_SALUD["_tk_salud_evaluar"]
AHORA = datetime(2026, 8, 5, 12, 0, 0)


class TestLatidoDeRecepcion(unittest.TestCase):
    def test_todo_bien(self):
        v = _evaluar({"errores_consecutivos": 0,
                      "ultimo_intento_at": AHORA - timedelta(minutes=1),
                      "ultimo_ok_at": AHORA - timedelta(minutes=1),
                      "ultimo_correo_at": AHORA - timedelta(hours=3)}, ahora=AHORA)
        self.assertEqual("ok", v["estado"])

    def test_un_solo_error_ya_es_critico(self):
        """No se espera a que "se acumulen" errores: si el último intento
        falló, los correos de clientes NO están entrando. Ese fue el estado
        real durante 12 días."""
        v = _evaluar({"errores_consecutivos": 1,
                      "ultimo_intento_at": AHORA,
                      "ultimo_ok_at": AHORA - timedelta(days=12),
                      "ultimo_error": "[AUTHENTICATIONFAILED] Invalid credentials",
                      "ultimo_correo_at": AHORA - timedelta(days=12)}, ahora=AHORA)
        self.assertEqual("critico", v["estado"])
        self.assertIn("AUTHENTICATIONFAILED", v["detalle"])

    def test_el_escenario_exacto_de_los_12_dias(self):
        """Conexión aparentemente sana pero 12 días sin un solo correo: aunque
        no hubiera error registrado, la pantalla TIENE que decir algo."""
        v = _evaluar({"errores_consecutivos": 0,
                      "ultimo_intento_at": AHORA,
                      "ultimo_ok_at": AHORA,
                      "ultimo_correo_at": AHORA - timedelta(days=12)}, ahora=AHORA)
        self.assertEqual("alerta", v["estado"])
        self.assertIn("12", v["titulo"])

    def test_fin_de_semana_largo_no_dispara_alarma(self):
        # ILUS no opera sábado/domingo: 4 días sin correos es normal.
        v = _evaluar({"errores_consecutivos": 0,
                      "ultimo_intento_at": AHORA,
                      "ultimo_ok_at": AHORA,
                      "ultimo_correo_at": AHORA - timedelta(days=4)}, ahora=AHORA)
        self.assertEqual("ok", v["estado"])

    def test_arranque_en_frio_no_es_una_falla(self):
        v = _evaluar({}, ahora=AHORA)
        self.assertEqual("desconocido", v["estado"])

    def test_conecta_pero_nunca_entro_un_correo(self):
        v = _evaluar({"errores_consecutivos": 0, "ultimo_intento_at": AHORA,
                      "ultimo_ok_at": AHORA, "ultimo_correo_at": None}, ahora=AHORA)
        self.assertEqual("alerta", v["estado"])

    def test_fila_none_no_revienta(self):
        self.assertEqual("desconocido", _evaluar(None, ahora=AHORA)["estado"])

    def test_error_sin_texto_igual_da_un_mensaje_util(self):
        v = _evaluar({"errores_consecutivos": 3, "ultimo_intento_at": AHORA,
                      "ultimo_error": None}, ahora=AHORA)
        self.assertEqual("critico", v["estado"])
        self.assertTrue(v["detalle"].strip())


# ══════════════════════════════════════════════════════════════════════
#  4. Barrido completo contra un buzón IMAP FALSO
# ══════════════════════════════════════════════════════════════════════
class BuzonProhibido(AssertionError):
    """Se lanza si el código intenta MODIFICAR el buzón. Es la casilla de
    trabajo real de Daniel: solo lectura, sin excepciones."""


class FakeIMAP:
    """Buzón IMAP de mentira. Vigila las reglas innegociables:
    - select() tiene que ser readonly=True
    - fetch() tiene que usar BODY.PEEK (BODY[] o RFC822 marcan \\Seen)
    - store/copy/move/expunge/delete: prohibidos
    """

    def __init__(self, mensajes):
        self.mensajes = mensajes          # lista de bytes (mensajes crudos)
        self.select_readonly = None
        self.fetch_specs = []
        self.search_criterio = None
        self.logout_llamado = False

    # ── comandos permitidos ──
    def login(self, user, pwd):
        return ("OK", [b"LOGIN completed"])

    def select(self, buzon, readonly=False):
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

    # ── comandos que JAMÁS deben llamarse ──
    def store(self, *a, **k):
        raise BuzonProhibido("store() — marcaría el correo como leído")

    def copy(self, *a, **k):
        raise BuzonProhibido("copy()")

    def move(self, *a, **k):
        raise BuzonProhibido("move()")

    def expunge(self, *a, **k):
        raise BuzonProhibido("expunge()")

    def uid(self, cmd, *a):
        if cmd.upper() in ("STORE", "COPY", "MOVE", "EXPUNGE"):
            raise BuzonProhibido(f"uid({cmd})")
        raise BuzonProhibido("uid() no se usa")


def _construir_mail(asunto, de, texto, message_id, cuando=None, adjunto=None):
    m = EmailMessage()
    m["Subject"] = asunto
    m["From"] = de
    m["To"] = BUZON
    m["Message-ID"] = message_id
    m["Date"] = format_datetime(cuando or datetime(2026, 7, 28, 15, 30,
                                                   tzinfo=timezone.utc))
    m.set_content(texto)
    if adjunto:
        nombre, contenido, mime = adjunto
        mayor, menor = mime.split("/", 1)
        m.add_attachment(contenido, maintype=mayor, subtype=menor, filename=nombre)
    return m.as_bytes()


class Banco:
    """MySQL de mentira: guarda lo que se escribió para poder afirmarlo."""

    def __init__(self, tickets, ya_ingeridos):
        self.tickets = tickets            # numero -> dict
        self.ya_ingeridos = set(ya_ingeridos)
        self.ejecutados = []              # (sql, params)

    def fetchone(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM tk_tickets WHERE numero_ticket" in s:
            return self.tickets.get(params[0])
        if "FROM tk_mail_ingeridos WHERE message_id" in s:
            return {"message_id": params[0]} if params[0] in self.ya_ingeridos else None
        return None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.ejecutados.append((s, params))
        if "INSERT IGNORE INTO tk_mail_ingeridos" in s:
            self.ya_ingeridos.add(params[0])
        return 1

    # ── ayudas de aserción ──
    def escrituras(self):
        return [s for s, _ in self.ejecutados]

    def cuenta(self, fragmento):
        return sum(1 for s in self.escrituras() if fragmento in s)


def _correr_barrido(mensajes, tickets, ya_ingeridos=(), **kw):
    """Ejecuta _tk_leer_correo_entrante contra el buzón falso.
    Devuelve (resumen, banco, imap, efectos)."""
    banco = Banco(tickets, ya_ingeridos)
    imap = FakeIMAP(mensajes)
    efectos = {"log": [], "subidas": [], "notifs": [], "salud": [], "reabiertos": []}

    def _tk_log(ticket_id, tipo, contenido, **k):
        efectos["log"].append({"ticket_id": ticket_id, "tipo": tipo,
                               "contenido": contenido, **k})
        return 900 + len(efectos["log"])

    def _uploader_upload(fs, folder=None, resource_type=None):
        efectos["subidas"].append(fs.filename)
        return {"secure_url": "https://gcs/x/" + fs.filename, "public_id": "pid"}

    def _mant_notificar(destino, tipo, titulo, **k):
        efectos["notifs"].append(titulo)
        return 1

    ns = _ns_base()
    ns.update({
        "imaplib": types.SimpleNamespace(IMAP4_SSL=lambda host, port: imap),
        "_tk_imap_creds": lambda: (BUZON, "clave-app"),
        "_tk_imap_host": lambda: "imap.gmail.com",
        "mysql_fetchone": banco.fetchone,
        "mysql_execute": banco.execute,
        "_tk_log": _tk_log,
        "_uploader_upload": _uploader_upload,
        "_EXT_PERMITIDAS": {".pdf", ".jpg", ".png"},
        "MAX_ADJUNTO_MB": 25,
        "_tk_reabrir_si_cerrado": lambda tid, est: efectos["reabiertos"].append(tid),
        "ctx": {"_mant_notificar": _mant_notificar},
        "_fmt_dt": lambda v, only_date=False: (v.strftime("%d/%m/%Y %H:%M")
                                               if hasattr(v, "strftime") else ""),
        "_tk_salud_registrar": lambda ok, **k: efectos["salud"].append((ok, k)),
    })
    exec(_fuente_asignacion("_TK_NUM_TICKET_RE"), ns)
    exec(_fuente_asignacion("_TK_QUOTE_RE"), ns)
    exec(_fuente_funcion("_tk_clasificar_correo"), ns)
    exec(_fuente_funcion("_tk_extraer_cuerpo_mail"), ns)
    exec(_fuente_funcion("_tk_leer_correo_entrante"), ns)
    resumen = ns["_tk_leer_correo_entrante"](**kw)
    return resumen, banco, imap, efectos


TICKET_42 = {"id": 42, "numero_ticket": "TK-2026-00042",
             "nombre_contacto": "Ana Cliente", "empresa": "Gimnasio Sur",
             "estado": "open"}

# El escenario completo, tal como se ve un buzón real de esos 12 días.
MSG_NUEVO = _construir_mail(
    "Re: [TK-2026-00042] Falla trotadora", "ana@gimnasiosur.cl",
    "Sigue sin andar, por favor vengan.", "<nuevo-1@mail.gmail.com>")
MSG_YA = _construir_mail(
    "Re: [TK-2026-00042] Falla trotadora", "ana@gimnasiosur.cl",
    "Ya lo habia mandado antes.", "<viejo-1@mail.gmail.com>")
MSG_SIN_TICKET = _construir_mail(
    "Re: [TK-2026-99999] Otro asunto", "otro@empresa.cl",
    "Hola?", "<huerfano-1@mail.gmail.com>")
MSG_PROPIO = _construir_mail(
    "[TK-2026-00042] Respuesta de ILUS", BUZON,
    "Estimada Ana, ya agendamos.", "<propio-1@mail.gmail.com>")
MSG_ADJUNTO = _construir_mail(
    "Re: [TK-2026-00042] Foto de la falla", "ana@gimnasiosur.cl",
    "Adjunto la foto.", "<adjunto-1@mail.gmail.com>",
    adjunto=("falla.jpg", b"\xff\xd8\xff\xe0fake-jpeg", "image/jpeg"))
MSG_NO_ES_NUESTRO = _construir_mail(
    "TK FRESHDESK newsletter", "marketing@otracosa.com",
    "Promo del mes", "<spam-1@mail.gmail.com>")

TODOS = [MSG_NUEVO, MSG_YA, MSG_SIN_TICKET, MSG_PROPIO, MSG_ADJUNTO,
         MSG_NO_ES_NUESTRO]
YA_INGERIDOS = ["<viejo-1@mail.gmail.com>"]


class TestVistaPrevia(unittest.TestCase):
    """dry_run: mira el buzón y clasifica, sin escribir NADA."""

    def setUp(self):
        self.resumen, self.banco, self.imap, self.efectos = _correr_barrido(
            TODOS, {"TK-2026-00042": TICKET_42}, YA_INGERIDOS,
            dias=20, max_correos=200, dry_run=True, detalle=True,
            notificar="resumen")

    def test_no_escribe_absolutamente_nada(self):
        self.assertEqual([], self.banco.ejecutados,
                         "la vista previa NO puede escribir en la base")
        self.assertEqual([], self.efectos["log"])
        self.assertEqual([], self.efectos["subidas"])
        self.assertEqual([], self.efectos["notifs"])
        self.assertEqual([], self.efectos["reabiertos"])

    def test_cuenta_correcta_de_cada_categoria(self):
        r = self.resumen
        self.assertTrue(r["ok"])
        self.assertTrue(r["dry_run"])
        self.assertEqual(2, r["ingresados"], "el nuevo + el que trae adjunto")
        self.assertEqual(1, r["duplicados"], "el que ya estaba ingerido")
        self.assertEqual(1, r["sin_ticket"], "TK-2026-99999 no existe")
        self.assertEqual(1, r["propios"], "el enviado por nosotros")
        self.assertEqual(0, r["errores"])

    def test_el_que_no_es_nuestro_ni_aparece(self):
        # "TK FRESHDESK" no es un ticket: no ensucia la lista.
        asuntos = [f["asunto"] for f in self.resumen["detalle"]]
        self.assertNotIn("TK FRESHDESK newsletter", asuntos)

    def test_el_detalle_dice_de_quien_y_de_que_ticket(self):
        porestado = {f["estado"]: f for f in self.resumen["detalle"]}
        self.assertEqual("ana@gimnasiosur.cl", porestado["nuevo"]["correo"])
        self.assertEqual("TK-2026-00042", porestado["nuevo"]["numero"])
        self.assertEqual("TK-2026-99999", porestado["sin_ticket"]["numero"])
        self.assertTrue(porestado["ya_ingerido"]["nota"])

    def test_la_fecha_va_en_formato_chileno_no_ISO(self):
        # REGLA #6: nunca ISO crudo ni inglés en la UI.
        for f in self.resumen["detalle"]:
            if f["fecha"]:
                self.assertRegex(f["fecha"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")

    def test_solo_baja_cabeceras(self):
        # No tiene sentido bajarse 20 MB de adjuntos para una previa.
        self.assertTrue(all("HEADER" in s for s in self.imap.fetch_specs),
                        self.imap.fetch_specs)

    def test_la_ventana_de_dias_llega_al_24_de_julio(self):
        # 20 días hacia atrás desde hoy cubre el inicio del apagón.
        self.assertIn("SINCE", self.imap.search_criterio)
        self.assertIn('SUBJECT "TK-"', self.imap.search_criterio)


class TestIngestaReal(unittest.TestCase):
    """dry_run=False: ingresa SOLO lo que falta."""

    def setUp(self):
        self.resumen, self.banco, self.imap, self.efectos = _correr_barrido(
            TODOS, {"TK-2026-00042": TICKET_42}, YA_INGERIDOS,
            dias=20, max_correos=200, dry_run=False, detalle=True,
            notificar="resumen")

    def test_ingresa_los_dos_que_faltaban(self):
        self.assertEqual(2, self.resumen["ingresados"])
        self.assertEqual(2, len(self.efectos["log"]))
        for m in self.efectos["log"]:
            self.assertEqual(42, m["ticket_id"])
            self.assertEqual("client_message", m["tipo"])
            self.assertFalse(m["es_interno"], "es mensaje del CLIENTE")

    def test_no_duplica_el_que_ya_estaba(self):
        cuerpos = [m["contenido"] for m in self.efectos["log"]]
        self.assertFalse(any("Ya lo habia mandado antes" in c for c in cuerpos))

    def test_no_ingresa_el_correo_propio(self):
        cuerpos = [m["contenido"] for m in self.efectos["log"]]
        self.assertFalse(any("ya agendamos" in c for c in cuerpos))

    def test_el_adjunto_se_sube_y_se_registra(self):
        self.assertEqual(["falla.jpg"], self.efectos["subidas"])
        self.assertEqual(1, self.resumen["adjuntos"])
        self.assertEqual(1, self.banco.cuenta("INSERT INTO tk_adjuntos"))

    def test_marca_ingerido_antes_de_subir_adjuntos(self):
        """Si el proceso muere subiendo un adjunto a GCS, lo peor que puede
        pasar es perder el adjunto (el correo sigue en el buzón), NUNCA
        duplicar el mensaje en la conversación del cliente."""
        orden = self.banco.escrituras()
        i_marca = next(i for i, s in enumerate(orden)
                       if "INSERT IGNORE INTO tk_mail_ingeridos" in s)
        i_adj = next(i for i, s in enumerate(orden) if "INSERT INTO tk_adjuntos" in s)
        self.assertLess(i_marca, i_adj)

    def test_un_adjunto_que_no_se_puede_guardar_queda_escrito_en_el_ticket(self):
        """Antes, un adjunto con extensión no permitida se descartaba con un
        `continue` mudo: el correo quedaba ingerido y nadie sabía que el
        cliente había mandado la foto de la falla."""
        msg = _construir_mail(
            "Re: [TK-2026-00042] con adjunto raro", "ana@gimnasiosur.cl",
            "Les mando el video.", "<raro-1@mail.gmail.com>",
            adjunto=("evidencia.heic", b"datos-binarios", "image/heic"))
        _, _, _, ef = _correr_barrido([msg], {"TK-2026-00042": TICKET_42},
                                      dias=20, dry_run=False)
        self.assertEqual(1, len(ef["log"]))
        cuerpo = ef["log"][0]["contenido"]
        self.assertIn("evidencia.heic", cuerpo)
        self.assertIn("no se pudieron guardar", cuerpo)
        self.assertEqual([], ef["subidas"])

    def test_adjunto_demasiado_grande_tambien_se_avisa(self):
        grande = b"x" * (26 * 1024 * 1024)
        msg = _construir_mail(
            "Re: [TK-2026-00042] pesado", "ana@gimnasiosur.cl",
            "Va el archivo.", "<pesado-1@mail.gmail.com>",
            adjunto=("manual.pdf", grande, "application/pdf"))
        _, _, _, ef = _correr_barrido([msg], {"TK-2026-00042": TICKET_42},
                                      dias=20, dry_run=False)
        cuerpo = ef["log"][0]["contenido"]
        self.assertIn("manual.pdf", cuerpo)
        self.assertIn("25 MB", cuerpo)

    def test_adjunto_valido_no_ensucia_el_cuerpo(self):
        cuerpos = [m["contenido"] for m in self.efectos["log"]]
        con_adjunto = [c for c in cuerpos if "Adjunto la foto" in c]
        self.assertTrue(con_adjunto)
        self.assertNotIn("no se pudieron guardar", con_adjunto[0])

    def test_usa_la_fecha_real_del_correo_no_la_del_barrido(self):
        for m in self.efectos["log"]:
            self.assertIsInstance(m["message_date"], datetime)
            self.assertEqual(2026, m["message_date"].year)

    def test_una_sola_campanita_de_resumen_no_una_por_correo(self):
        self.assertEqual(1, len(self.efectos["notifs"]))
        self.assertIn("Recuperación", self.efectos["notifs"][0])

    def test_modo_cada_deja_una_campanita_por_correo(self):
        _, _, _, ef = _correr_barrido(
            TODOS, {"TK-2026-00042": TICKET_42}, YA_INGERIDOS,
            dias=20, dry_run=False, notificar="cada")
        self.assertEqual(2, len(ef["notifs"]))

    def test_registra_el_latido_de_trafico(self):
        oks = [k for ok, k in self.efectos["salud"] if ok and k.get("ultimo_correo_at")]
        self.assertTrue(oks, "tiene que quedar registrado el último correo real")

    def test_segunda_pasada_no_reingresa_nada(self):
        """Idempotencia real: el dedup por Message-ID."""
        r2, _, _, ef2 = _correr_barrido(
            TODOS, {"TK-2026-00042": TICKET_42},
            list(self.banco.ya_ingeridos), dias=20, dry_run=False)
        self.assertEqual(0, r2["ingresados"])
        self.assertEqual([], ef2["log"])


class TestReglasInnegociablesDelBuzon(unittest.TestCase):
    """Es la casilla de trabajo REAL de Daniel."""

    def test_select_es_readonly(self):
        _, _, imap, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                        YA_INGERIDOS, dias=20, dry_run=False)
        self.assertIs(True, imap.select_readonly)

    def test_todo_fetch_usa_body_peek(self):
        _, _, imap, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                        YA_INGERIDOS, dias=20, dry_run=False)
        self.assertTrue(imap.fetch_specs)
        for s in imap.fetch_specs:
            self.assertIn("BODY.PEEK", s)

    def test_cierra_la_sesion(self):
        _, _, imap, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                        YA_INGERIDOS, dias=20, dry_run=True)
        self.assertTrue(imap.logout_llamado)

    def test_el_codigo_fuente_no_manda_correo_al_ingerir(self):
        """FRENO EXPLÍCITO: un barrido de recuperación toca correos de
        clientes REALES. Si la ingesta disparara un acuse de recibo, 12 días
        de atraso se convertirían en decenas de correos inesperados a
        clientes. Esta prueba falla si alguien agrega un envío ahí."""
        fuente = _fuente_funcion("_tk_leer_correo_entrante")
        for prohibido in ("_send_ilus_email", "_ilus_email_master", "smtplib",
                          "_email_queue", "sendmail", "SMTP("):
            self.assertNotIn(prohibido, fuente,
                             f"la ingesta NO puede enviar correo ({prohibido})")

    def test_el_codigo_fuente_no_modifica_el_buzon(self):
        fuente = _fuente_funcion("_tk_leer_correo_entrante")
        for prohibido in (".store(", ".copy(", ".move(", ".expunge(",
                          "\\Seen", "readonly=False"):
            self.assertNotIn(prohibido, fuente)
        self.assertIn("readonly=True", fuente)


class TestErroresYBordes(unittest.TestCase):
    def test_sin_credenciales_devuelve_error_y_marca_el_latido_rojo(self):
        ns = _ns_base()
        salud = []
        ns.update({
            "imaplib": types.SimpleNamespace(IMAP4_SSL=lambda *a: None),
            "_tk_imap_creds": lambda: ("", ""),
            "_tk_imap_host": lambda: "imap.gmail.com",
            "mysql_fetchone": lambda *a, **k: None,
            "mysql_execute": lambda *a, **k: 1,
            "_tk_salud_registrar": lambda ok, **k: salud.append((ok, k)),
            "_fmt_dt": lambda v, only_date=False: "",
        })
        exec(_fuente_asignacion("_TK_NUM_TICKET_RE"), ns)
        exec(_fuente_funcion("_tk_clasificar_correo"), ns)
        exec(_fuente_funcion("_tk_leer_correo_entrante"), ns)
        r = ns["_tk_leer_correo_entrante"](dias=20)
        self.assertFalse(r["ok"])
        self.assertTrue(salud and salud[0][0] is False)

    def test_imap_caido_devuelve_error_legible_y_no_lanza(self):
        def _explota(host, port):
            raise OSError("[AUTHENTICATIONFAILED] Invalid credentials")
        ns = _ns_base()
        salud = []
        ns.update({
            "imaplib": types.SimpleNamespace(IMAP4_SSL=_explota),
            "_tk_imap_creds": lambda: (BUZON, "clave"),
            "_tk_imap_host": lambda: "imap.gmail.com",
            "mysql_fetchone": lambda *a, **k: None,
            "mysql_execute": lambda *a, **k: 1,
            "_tk_salud_registrar": lambda ok, **k: salud.append((ok, k)),
            "_fmt_dt": lambda v, only_date=False: "",
        })
        exec(_fuente_asignacion("_TK_NUM_TICKET_RE"), ns)
        exec(_fuente_funcion("_tk_clasificar_correo"), ns)
        exec(_fuente_funcion("_tk_leer_correo_entrante"), ns)
        r = ns["_tk_leer_correo_entrante"](dias=20)
        self.assertFalse(r["ok"])
        self.assertIn("IMAP no disponible", r["error"])
        self.assertEqual(1, len(salud))
        self.assertFalse(salud[0][0])
        self.assertIn("AUTHENTICATIONFAILED", salud[0][1]["error"])

    def test_avisa_cuando_el_buzon_trae_mas_correos_que_el_tope(self):
        """El barrido se queda con los MÁS RECIENTES. En una recuperación eso
        deja fuera justo los más viejos, que es lo que se quiere rescatar —
        así que hay que decirlo, no callarlo."""
        r, _, _, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                     YA_INGERIDOS, dias=20, max_correos=2,
                                     dry_run=True, detalle=True)
        self.assertTrue(r["truncado"])
        self.assertEqual(len(TODOS), r["truncado_total"])
        self.assertEqual(2, r["vistos"])

    def test_correo_sin_message_id_no_se_duplica_entre_previa_e_ingesta(self):
        """Sin Message-ID se usa un hash del correo completo. Si la previa
        hasheara solo la cabecera, diría "nuevo" para algo ya ingerido."""
        m = EmailMessage()
        m["Subject"] = "Re: [TK-2026-00042] sin id"
        m["From"] = "ana@gimnasiosur.cl"
        m["Date"] = format_datetime(datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc))
        m.set_content("Hola, sin message id.")
        crudo = m.as_bytes()

        _, banco, _, _ = _correr_barrido([crudo], {"TK-2026-00042": TICKET_42},
                                         dias=20, dry_run=False)
        ingerido = list(banco.ya_ingeridos)
        self.assertEqual(1, len(ingerido))
        self.assertTrue(ingerido[0].startswith("sin-id-"))

        r2, _, _, _ = _correr_barrido([crudo], {"TK-2026-00042": TICKET_42},
                                      ingerido, dias=20, dry_run=True,
                                      detalle=True)
        self.assertEqual(1, r2["duplicados"], "la previa tiene que verlo ya ingerido")
        self.assertEqual(0, r2["ingresados"])

    def test_presupuesto_de_tiempo_corta_limpio_y_lo_dice(self):
        """Un barrido de recuperación con adjuntos pesados puede durar
        minutos y morir por timeout HTTP. Preferimos cortar, devolver lo
        hecho y avisar: reintentar retoma donde quedó (dedup)."""
        r, _, _, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                     YA_INGERIDOS, dias=20, dry_run=True,
                                     detalle=True, max_segundos=0.0001)
        self.assertTrue(r["parcial"])
        self.assertTrue(r["ok"], "cortar por tiempo no es un error")

    def test_sin_presupuesto_no_marca_parcial(self):
        r, _, _, _ = _correr_barrido(TODOS, {"TK-2026-00042": TICKET_42},
                                     YA_INGERIDOS, dias=20, dry_run=True)
        self.assertFalse(r["parcial"])

    def test_reabre_el_ticket_cerrado_cuando_el_cliente_responde(self):
        cerrado = dict(TICKET_42, estado="closed")
        _, _, _, ef = _correr_barrido([MSG_NUEVO], {"TK-2026-00042": cerrado},
                                      dias=20, dry_run=False)
        self.assertEqual([42], ef["reabiertos"])

    def test_la_previa_no_reabre_nada(self):
        cerrado = dict(TICKET_42, estado="closed")
        _, _, _, ef = _correr_barrido([MSG_NUEVO], {"TK-2026-00042": cerrado},
                                      dias=20, dry_run=True)
        self.assertEqual([], ef["reabiertos"])


# ══════════════════════════════════════════════════════════════════════
#  5. La alerta que rompe el silencio
# ══════════════════════════════════════════════════════════════════════
def _correr_alerta(errores, ultima_alerta_at=None, apagado=False,
                   rowcount=1, hay_email=True):
    enviados, campanas, ejecutados = [], [], []

    def _fetchone(sql, params=None):
        s = " ".join(sql.split())
        if "FROM tk_mail_salud WHERE id=1" in s:
            return {"errores_consecutivos": errores,
                    "ultima_alerta_at": ultima_alerta_at}
        if "clave='mail_alerta_off'" in s:
            return {"valor": "1"} if apagado else None
        return None

    def _enviar(dest, asunto, html, **k):
        enviados.append({"dest": dest, "asunto": asunto, "html": html, **k})
        return True

    ns = _ns_base()
    ns.update({
        "mysql_fetchone": _fetchone,
        "mysql_execute": lambda sql, params=None: ejecutados.append(sql),
        "ctx": {"_mant_notificar":
                lambda destino, tipo, titulo, **k: campanas.append(titulo),
                "mysql_execute_returning_rowcount": lambda sql, params=None: rowcount},
        "_send_ilus_email": _enviar if hay_email else None,
        "_ilus_email_master": None,
        "_tk_reply_to": lambda: "soporte@sphs.cl",
        "_html_mod": __import__("html"),
    })
    exec(_fuente_asignacion("_TK_MAIL_ALERTA_ERRORES"), ns)
    exec(_fuente_asignacion("_TK_MAIL_ALERTA_HORAS"), ns)
    exec(_fuente_funcion("_tk_salud_alertar"), ns)
    ns["_tk_salud_alertar"]("[AUTHENTICATIONFAILED] Invalid credentials")
    return enviados, campanas


class TestAlertaDeCaida(unittest.TestCase):
    def test_un_error_aislado_no_molesta_a_nadie(self):
        enviados, campanas = _correr_alerta(errores=1)
        self.assertEqual([], enviados)
        self.assertEqual([], campanas)

    def test_tres_errores_seguidos_avisan_por_campana_y_correo(self):
        enviados, campanas = _correr_alerta(errores=3)
        self.assertEqual(1, len(campanas))
        self.assertEqual(1, len(enviados))

    def test_el_correo_de_alerta_jamas_puede_volver_a_entrar_como_ticket(self):
        """Si el asunto trajera un TK-, el propio lector lo re-ingeriría como
        mensaje de un ticket. Además va a NUESTRO buzón, nunca a un cliente."""
        enviados, _ = _correr_alerta(errores=5)
        self.assertNotIn("TK-", enviados[0]["asunto"])
        self.assertEqual("soporte@sphs.cl", enviados[0]["dest"])

    def test_el_correo_dice_que_los_correos_no_se_perdieron(self):
        enviados, _ = _correr_alerta(errores=5)
        self.assertIn("Recuperar correos", enviados[0]["html"])
        self.assertIn("IMAP", enviados[0]["html"])

    def test_throttle_no_manda_dos_correos_seguidos(self):
        # rowcount=0 = el UPDATE condicional no tomó el candado (ya se avisó).
        enviados, campanas = _correr_alerta(errores=9, rowcount=0)
        self.assertEqual([], enviados)
        self.assertEqual(1, len(campanas), "la campana interna sí se mantiene")

    def test_kill_switch_apaga_el_correo_pero_no_la_campana(self):
        enviados, campanas = _correr_alerta(errores=9, apagado=True)
        self.assertEqual([], enviados)
        self.assertEqual(1, len(campanas))

    def test_sin_motor_de_correo_no_revienta(self):
        enviados, campanas = _correr_alerta(errores=9, hay_email=False)
        self.assertEqual([], enviados)
        self.assertEqual(1, len(campanas))

    def test_el_destinatario_nunca_sale_de_datos_del_cliente(self):
        fuente = _fuente_funcion("_tk_salud_alertar")
        for prohibido in ("from_email", "ticket[", "nombre_contacto", "empresa"):
            self.assertNotIn(prohibido, fuente,
                             "la alerta NO puede dirigirse a un cliente")
        self.assertIn("TK_ALERTA_MAIL_TO", fuente)
        self.assertIn("_tk_reply_to", fuente)


class TestEsquemaSobreviveASkipMigrations(unittest.TestCase):
    """Producción corre con ILUS_SKIP_MIGRATIONS=1: los init_* NO se ejecutan.
    Todo esquema nuevo tiene que vivir en un _ensure_* llamado SIEMPRE al
    registrar el módulo. Si alguien lo mueve a un bloque gateado, esto falla
    y no se descubre con un 500 en producción."""

    def test_existe_el_ensure_y_crea_la_tabla_de_forma_idempotente(self):
        fuente = _fuente_funcion("_ensure_tk_mail_salud")
        self.assertIn("CREATE TABLE IF NOT EXISTS tk_mail_salud", fuente)
        self.assertIn("INSERT IGNORE INTO tk_mail_salud", fuente)

    def test_se_llama_en_el_arranque_del_modulo(self):
        # La llamada tiene que estar dentro del bloque `with app.app_context()`
        # que corre siempre al registrar las rutas, junto a los otros _ensure_*.
        self.assertIn("_ensure_tk_mail_salud()", SRC)
        i_def = SRC.index("def _ensure_tk_mail_salud")
        i_call = SRC.index("_ensure_tk_mail_salud()", SRC.index("with app.app_context():"))
        self.assertGreater(i_call, i_def, "se llama después de definirla")

    def test_no_hay_env_var_que_apague_el_esquema(self):
        # Se mira el CÓDIGO, no el docstring (que sí menciona la env var
        # para explicar por qué existe este patrón).
        for nodo in ast.walk(_ARBOL):
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "_ensure_tk_mail_salud":
                cuerpo = nodo.body[1:] if (nodo.body and isinstance(nodo.body[0], ast.Expr)
                                           and isinstance(nodo.body[0].value, ast.Constant)
                                           ) else nodo.body
                codigo = "\n".join(ast.unparse(x) for x in cuerpo)
                self.assertNotIn("SKIP_MIGRATIONS", codigo)
                self.assertNotIn("environ", codigo)
                return
        self.fail("no se encontró _ensure_tk_mail_salud")


class TestSinCredencialesElLatidoSePintaRojo(unittest.TestCase):
    """El autopoll antes hacía `return` mudo si no había credenciales: la
    recepción estaba muerta y la pantalla no mostraba nada."""

    def test_el_autopoll_registra_la_falta_de_credenciales(self):
        fuente = _fuente_funcion("_tk_autopoll_correo")
        self.assertIn("_tk_salud_registrar", fuente)
        # Y el chequeo de credenciales va DESPUÉS del throttle, para no hacer
        # un UPDATE por cada vista de la bandeja.
        i_throttle = fuente.index("_TK_AUTOPOLL_SEG")
        i_creds = fuente.index("_tk_imap_creds")
        self.assertLess(i_throttle, i_creds)


if __name__ == "__main__":
    unittest.main(verbosity=2)

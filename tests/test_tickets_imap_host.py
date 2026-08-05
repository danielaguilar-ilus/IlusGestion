"""Pruebas de _tk_imap_host() — el servidor de ENTRADA de los tickets.

Daniel, 2026-08-05: "yo cambio el correo, le pongo la clave y chao... déjalo
dinámico por el front".

CONTEXTO DEL BUG QUE ESTO PREVIENE: entre el 24-jul y el 5-ago-2026 los
correos de clientes dejaron de convertirse en tickets. Doce días. Nadie lo
notó porque el ENVÍO seguía funcionando perfecto — el envío leía la config
guardada en /comunicaciones y la lectura del buzón leía variables de entorno.
Se rotó la clave en la pantalla y las dos fuentes quedaron desincronizadas.

Ese mismo riesgo existía con el HOST: estaba clavado a "imap.gmail.com". Si
alguna vez se cambia el proveedor en la pantalla, el envío se movería y la
recepción se quedaría apuntando a Gmail. Mismo desastre, al revés. Estas
pruebas fijan que el host de entrada SIEMPRE se derive del de salida.

No se importa tickets_module directo: register_tickets_routes necesita una
app Flask y un ctx completo. Se extrae la función con `ast` y se ejecuta
aislada — mismo patrón ya usado en tests/test_transporte_scheduler.py y
tests/test_password_email_templates.py.

Correr con:  py -m unittest tests.test_tickets_imap_host -v
"""
import ast
import os
import unittest

SRC = open("tickets_module.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(SRC)


def _extraer(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró '{nombre}' en tickets_module.py")


def _host_con(smtp_host, env_host=None):
    """Ejecuta _tk_imap_host() como si la pantalla tuviera ese servidor SMTP.

    smtp_host=None simula que no hay fila guardada en la BD (cae a env).
    """
    ns = {"os": os, "ctx": {}}
    if smtp_host is not None:
        ns["ctx"] = {"_get_smtp_cfg": lambda: {"smtp_host": smtp_host}}
    prev = os.environ.get("SMTP_HOST")
    if env_host is None:
        os.environ.pop("SMTP_HOST", None)
    else:
        os.environ["SMTP_HOST"] = env_host
    try:
        exec(_extraer("_tk_imap_host"), ns)
        return ns["_tk_imap_host"]()
    finally:
        if prev is None:
            os.environ.pop("SMTP_HOST", None)
        else:
            os.environ["SMTP_HOST"] = prev


class TestProveedoresReales(unittest.TestCase):
    def test_gmail_que_es_lo_que_usa_ilus_hoy(self):
        self.assertEqual("imap.gmail.com", _host_con("smtp.gmail.com"))

    def test_office365_no_es_imap_punto_office365(self):
        # El caso que se rompería con un reemplazo ingenuo de prefijo:
        # Microsoft NO expone "imap.office365.com".
        self.assertEqual("outlook.office365.com", _host_con("smtp.office365.com"))

    def test_zoho(self):
        self.assertEqual("imap.zoho.com", _host_con("smtp.zoho.com"))

    def test_dominio_propio_con_prefijo_mail_sirve_ambos(self):
        self.assertEqual("mail.sphs.cl", _host_con("mail.sphs.cl"))

    def test_host_sin_prefijo_conocido_usa_la_convencion(self):
        self.assertEqual("imap.sphs.cl", _host_con("sphs.cl"))


class TestNormalizacion(unittest.TestCase):
    def test_mayusculas_y_espacios_no_rompen(self):
        self.assertEqual("imap.gmail.com", _host_con("  SMTP.GMAIL.COM  "))

    def test_si_ya_viene_como_imap_se_respeta(self):
        self.assertEqual("imap.gmail.com", _host_con("imap.gmail.com"))


class TestFuentesYRespaldo(unittest.TestCase):
    """El orden de prioridad tiene que ser el MISMO que el del envío:
    primero lo que Daniel guardó en la pantalla, después las env vars."""

    def test_lo_de_la_pantalla_le_gana_a_la_variable_de_entorno(self):
        # Este es exactamente el escenario que causó los 12 días de silencio:
        # la pantalla actualizada y la env var vieja.
        self.assertEqual("imap.zoho.com",
                         _host_con("smtp.zoho.com", env_host="smtp.gmail.com"))

    def test_sin_fila_en_bd_cae_a_la_variable_de_entorno(self):
        self.assertEqual("imap.zoho.com", _host_con(None, env_host="smtp.zoho.com"))

    def test_sin_nada_configurado_cae_a_gmail_como_siempre(self):
        self.assertEqual("imap.gmail.com", _host_con(None))

    def test_pantalla_vacia_no_deja_el_host_en_blanco(self):
        # Fila guardada pero con el host borrado: no debe devolver "imap."
        self.assertEqual("imap.gmail.com", _host_con(""))


class TestNuncaLanza(unittest.TestCase):
    def test_si_leer_la_config_explota_igual_devuelve_un_host_valido(self):
        def _explota():
            raise RuntimeError("BD caída")
        ns = {"os": os, "ctx": {"_get_smtp_cfg": _explota}}
        os.environ.pop("SMTP_HOST", None)
        exec(_extraer("_tk_imap_host"), ns)
        self.assertEqual("imap.gmail.com", ns["_tk_imap_host"]())


if __name__ == "__main__":
    unittest.main(verbosity=2)

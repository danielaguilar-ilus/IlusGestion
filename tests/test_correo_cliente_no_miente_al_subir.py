"""
2026-08-23. Al subir un manifiesto a SimpliRoute, el cliente recibia un
correo que decia:

    Asunto: "ILUS - Tu pedido FCV 11329 ya salio de bodega"
    Cuerpo: "Tu pedido ya salio de nuestras instalaciones y quedo en
             camino a ti."

Crear la visita en SimpliRoute NO significa nada de eso. Medido contra la
API real el 20-08-2026 (dias 13, 14, 19 y 20): de 15 visitas creadas por
ILUS, CERO se entregaron jamas. El despachador del courier puede no meterlas
nunca en una ruta y la carga sigue en bodega.

Verificado ademas que el correo NO estaba bloqueado: kill_switch email=true
en produccion, y salio al menos uno a un cliente real el 20-08.

DECISION DE DANIEL (23-08): no avisar en ese punto. El estado interno sigue
pasando a 'Entregado a transporte' (el tablero lo necesita), pero el cliente
recibe su aviso cuando el pedido de verdad se mueve.

FedEx NO se toca: ahi el aviso se dispara al generar la etiqueta y eso si
significa que FedEx ya tiene el envio (status OC = Order Created).
"""
import ast
import subprocess
import unittest


def _tree():
    with open("app.py", encoding="utf-8") as f:
        return ast.parse(f.read())


def _cuerpo(nombre, tree=None):
    tree = tree if tree is not None else _tree()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == nombre:
            return node
    raise AssertionError(f"no se encontro la funcion {nombre}")


def _fuente(nombre, tree=None):
    return ast.unparse(_cuerpo(nombre, tree))


def _norm(s):
    return s.replace('"', "'")


class TestLaSubidaNoAvisaAlCliente(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("tr_manifiesto_subir_simpliroute"))

    def test_el_estado_sigue_pasando_a_entregado_a_transporte(self):
        """El cambio de estado interno NO se toca: el item quedo delegado al
        courier y el tablero tiene que reflejarlo. Lo unico que cambia es a
        quien se le avisa."""
        self.assertIn("'Entregado a transporte'", self.src)
        self.assertIn("Visita creada en SimpliRoute", self.src)

    def test_no_se_notifica_al_cliente_al_crear_la_visita(self):
        i = self.src.index("Visita creada en SimpliRoute")
        fragmento = self.src[i:i + 200]
        self.assertIn("notify_cliente=False", fragmento)
        self.assertNotIn("notify_cliente=True", fragmento)


class TestFedexNoSeToco(unittest.TestCase):
    """El aviso de FedEx al generar la etiqueta SI es correcto: significa que
    FedEx ya tiene el envio en su sistema. No se toca (REGLA #4.2)."""

    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_app = f.read()

    def test_fedex_sigue_avisando_al_generar_la_etiqueta(self):
        """Los 2 call-sites de FedEx conservan notify_cliente=True."""
        encontrados = 0
        for i, linea in enumerate(self.src_app.splitlines()):
            if "'Entregado a transporte'" in linea and "fuente='sistema'" in linea:
                ventana = "\n".join(self.src_app.splitlines()[i:i + 4])
                if "notify_cliente=True" in ventana:
                    encontrados += 1
        self.assertGreaterEqual(
            encontrados, 2,
            "los avisos de FedEx al generar etiqueta deben seguir activos")


class TestElClienteNoQuedaEnSilencio(unittest.TestCase):
    """Quitar el aviso de la subida solo es honesto si el aviso REAL sigue
    llegando despues. El poller es quien lo manda."""

    @classmethod
    def setUpClass(cls):
        cls.src_poll = _norm(_fuente("_simpliroute_poll_batch"))

    def test_el_poller_sigue_notificando(self):
        self.assertIn("notify_cliente=_notify", self.src_poll)

    def test_el_poller_notifica_por_defecto(self):
        """_notify arranca en True; solo se apaga en los 2 casos puntuales
        que Daniel definio el 07-08 (re-vinculacion y checkout viejo)."""
        i = self.src_poll.index("_notify = True")
        self.assertGreater(i, 0)

    def test_el_traductor_de_estados_sigue_dando_en_ruta(self):
        """on_its_way -> 'En ruta' es el aviso que reemplaza al que se quita."""
        with open("simpliroute_client.py", encoding="utf-8") as f:
            src = f.read()
        i = src.index("def estado_ilus_from_visit")
        # Quote-agnostic: el archivo usa comillas dobles, no simples.
        cuerpo = _norm(src[i:i + 1800])
        self.assertIn("on_its_way", cuerpo)
        self.assertIn("'En ruta'", cuerpo)


class TestLaCancelacionSigueAvisando(unittest.TestCase):
    """Al cancelar una visita el aviso SI es honesto (el pedido volvio a
    preparacion). No entraba en el alcance de este cambio."""

    def test_la_cancelacion_conserva_su_aviso(self):
        src = _norm(_fuente("tr_item_simpliroute_cancelar"))
        i = src.index("Visita SimpliRoute cancelada")
        self.assertIn("notify_cliente=True", src[max(0, i - 200):i + 200])


class TestNoSeTocoNadaAjeno(unittest.TestCase):
    # Lo que este arreglo NO debe romper. Lista nombrada, NO "todo app.py":
    # comparar el diff completo contra origin/main es una asercion de revision
    # de PR, no un test de regresion -- sobre un archivo de 92mil lineas que
    # varias features tocan en paralelo siempre da falso positivo. La historia
    # completa (4 intentos fallidos) esta en
    # test_pantalla_despacho_semaforo.py::test_no_rompe_los_caminos_criticos.
    INTOCABLES = (
        "_tr_bulk_sync_erp_mysql",      # el cron
        "_transporte_scheduler_loop",    # el cron
        "_tr_notificar_cliente",         # el texto y el envio del correo
        "_simpliroute_poll_batch",       # quien avisa al cliente DESPUES
        "tr_item_simpliroute_cancelar",  # el otro aviso, que si es honesto
    )

    @classmethod
    def setUpClass(cls):
        cls.tree_local = _tree()
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    def test_no_rompe_los_caminos_criticos_vecinos(self):
        rotas = [fn for fn in self.INTOCABLES
                 if _fuente(fn, self.tree_local) != _fuente(fn, self.tree_main)]
        self.assertEqual(rotas, [], f"caminos criticos modificados: {rotas}")

    def test_el_texto_del_correo_no_se_toco(self):
        """No se reescribio el copy: se dejo de MANDAR en ese punto. El mismo
        texto sigue sirviendo para FedEx, donde si es cierto."""
        self.assertEqual(
            _fuente("_tr_notificar_cliente", self.tree_local),
            _fuente("_tr_notificar_cliente", self.tree_main))


if __name__ == "__main__":
    unittest.main()

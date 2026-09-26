"""Fase 5 (2026-09-26) -- compra a proveedor con ticket y seguimiento
(Daniel: "si no tiene stock, nos vayamos a los tickets, generemos un
ticket de atencion con los datos de los proveedores... con total
trazabilidad y viendo los estados"; "puedo hacer dos tickets... agrupar
las solicitudes de repuestos por proveedor").

Incluye la revision #2 (mismo dia) de hallazgos ALTA/MEDIA sobre el commit
214b05e9: recepcion parcial que SI mueve kardex real por cada entrega
(motivo_tipo propio 'recepcion_parcial', fuera del set idempotente),
incremento atomico de cantidad_recibida (MEDIA #2), _otrep_compra_elegible
corregida (MEDIA #3: stock_disponible ya neto, se compara contra 0, no
contra cantidad de nuevo), _otrep_compra_sincronizar con concurrencia y
cancelacion cuando no quedan lineas vivas (MEDIA #4).

Prueba funciones PURAS de app.py (sin BD ni Flask) extraidas con ast:

  _otrep_compra_elegible(s)
      Que solicitudes puede agrupar una Compra nueva.

  _otrep_recepcion_es_completa(cantidad_recibida, cantidad_requerida)
      La regla de "recepcion parcial" del endpoint POST
      .../compras/<cid>/recibir: si lo recibido ACUMULADO no alcanza lo
      pedido, la linea sigue 'pedido' con una nota en vez de pasar a
      'recibido'.

  _repstock_mover(...)
      El kardex real (Fase 3) -- se prueba con un cursor FALSO (mismo
      patron que tests/test_repuestos_kardex.py) que motivo_tipo=
      'recepcion_parcial' (entregas parciales de una Compra) NO es
      idempotente por diseno -- cada entrega inserta su propio movimiento,
      a diferencia de 'recepcion_proveedor'/'instalacion' -- y que dos
      entregas parciales seguidas suman el saldo fisico correctamente.

Las funciones/endpoints que SI dependen de Flask/MySQL de verdad
(repstock_compra_recibir, _otrep_cambiar_estado, repstock_compra_estado,
_otrep_crear_compra) se revisan por TEXTO FUENTE (ast.unparse, sin
ejecutar nada) para confirmar que los guards de la revision siguen
presentes -- mismo criterio que tests/test_repuestos_kardex.py.

Correr con:  py -m unittest tests.test_repuestos_compras_fase5
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _cargar_funcion(nombre):
    """Extrae UNA funcion de app.py por nombre y la ejecuta en un ambito
    aislado. Solo sirve para funciones PURAS (que no llaman a otros
    simbolos de app.py, ni a Flask/MySQL)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert nombre in ambito, f"no se encontro {nombre} en app.py"
    return ambito[nombre]


def _extraer_funcion_con_contexto(nombre, contexto=None):
    """Como _cargar_funcion, pero admite simbolos externos (ej. FakeCursor
    no hace falta aca -- _repstock_mover no llama a nada mas de app.py)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = dict(contexto or {})
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert nombre in ambito, f"no se encontro {nombre} en app.py"
    return ambito[nombre]


def _extraer_fuente_funcion(nombre):
    """Devuelve el CODIGO FUENTE (sin ejecutar nada) de una funcion top-level
    de app.py -- para revisar por texto que un guard/condicion sigue
    presente, cuando la funcion depende de Flask/MySQL y no es extraible/
    ejecutable de forma aislada. Mismo patron que
    tests/test_repuestos_kardex.py."""
    arbol = ast.parse(_leer(APP_PY))
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class FakeCursor:
    """Cursor falso: registra cada `execute(sql, params)` en `self.executed`
    y devuelve `fetchone()` en el orden de `self.respuestas` (una cola) --
    mismo FakeCursor que tests/test_repuestos_kardex.py."""

    def __init__(self, respuestas):
        self.executed = []
        self._respuestas = list(respuestas)

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._respuestas.pop(0) if self._respuestas else None


class TestCompraElegible(unittest.TestCase):
    """_otrep_compra_elegible: ver el docstring en app.py (2026-09-26,
    revision #2, hallazgo MEDIA #3) para la definicion completa.

    🔒 MEDIA #3: `stock_disponible` (armado en _otrep_fila) YA le resta al
    fisico de bodega el comprometido de TODAS las OT abiertas, INCLUYENDO
    la cantidad propia de esta misma solicitud -- comparalo de nuevo contra
    `cantidad` restaba dos veces. La comparacion correcta es simplemente
    `stock_disponible < 0` (bodega ya no alcanza ni para lo comprometido)."""

    @classmethod
    def setUpClass(cls):
        cls.elegible = staticmethod(_cargar_funcion("_otrep_compra_elegible"))

    def test_validado_con_disponible_negativo_es_elegible(self):
        # stock_disponible YA neto (incluye lo propio) -- negativo significa
        # que ni siquiera comprometiendo todo el fisico alcanza.
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "stock_disponible": -2}
        self.assertTrue(self.elegible(s))

    def test_validado_con_disponible_positivo_no_es_elegible(self):
        # Si el disponible (ya neto) alcanza y sobra, no hay nada que
        # comprarle a nadie -- REGLA de negocio (hallazgo MEDIA #9/#3).
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "stock_disponible": 5}
        self.assertFalse(self.elegible(s))

    def test_validado_disponible_exactamente_cero_no_es_elegible(self):
        # Frontera: 0 significa "justo alcanza" -- no es "falta stock".
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "stock_disponible": 0}
        self.assertFalse(self.elegible(s))

    def test_validado_disponible_apenas_bajo_cero_es_elegible(self):
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "stock_disponible": -0.5}
        self.assertTrue(self.elegible(s))

    def test_validado_sin_dato_de_stock_disponible_es_elegible(self):
        # Defensivo: si por lo que sea no vino stock_disponible (None), se
        # prefiere dejarla pasar (mejor ofrecerla de mas que esconderla).
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "stock_disponible": None}
        self.assertTrue(self.elegible(s))

    def test_solicitado_ya_no_es_elegible_bajo_ningun_caso(self):
        # 🔒 ALTA #3: se elimino el camino "solicitado ya ligado a
        # proveedor" -- ahora SIEMPRE hay que pasar por 'validado' primero,
        # sin excepcion, aunque tenga proveedor_id y no tenga stock ligado.
        s = {"estado": "solicitado", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_pedido_sin_compra_es_elegible_solo_para_adjuntar(self):
        # Ya se le pidio al proveedor ANTES de que existiera esta Compra
        # (Fase <5, individual): se adjunta a la Compra nueva.
        s = {"estado": "pedido", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertTrue(self.elegible(s))

    def test_pedido_con_compra_ya_asignada_no_es_elegible(self):
        # Ya esta en OTRA compra -- no se puede meter en una segunda.
        s = {"estado": "pedido", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": 42}
        self.assertFalse(self.elegible(s))

    def test_recibido_no_es_elegible(self):
        s = {"estado": "recibido", "repuesto_stock_id": 9, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_instalado_no_es_elegible(self):
        s = {"estado": "instalado", "repuesto_stock_id": 9, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_rechazado_no_es_elegible(self):
        s = {"estado": "rechazado", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))


class TestRecepcionEsCompleta(unittest.TestCase):
    """_otrep_recepcion_es_completa: regla de "recepcion parcial" del
    endpoint POST /repuestos/api/compras/<cid>/recibir."""

    @classmethod
    def setUpClass(cls):
        cls.completa = staticmethod(_cargar_funcion("_otrep_recepcion_es_completa"))

    def test_recibido_igual_a_lo_pedido_es_completa(self):
        self.assertTrue(self.completa(5, 5))

    def test_recibido_de_mas_es_completa(self):
        self.assertTrue(self.completa(6, 5))

    def test_recibido_de_menos_es_parcial(self):
        self.assertFalse(self.completa(3, 5))

    def test_tolerancia_de_punto_flotante(self):
        # 0.1 + 0.2 == 0.30000000000000004 en float -- la tolerancia evita
        # que una cantidad EXACTA quede marcada como parcial por un error
        # de redondeo binario.
        self.assertTrue(self.completa(0.1 + 0.2, 0.3))

    def test_cero_recibido_no_es_completa(self):
        self.assertFalse(self.completa(0, 5))

    def test_valor_no_numerico_no_revienta_y_no_es_completa(self):
        self.assertFalse(self.completa("no-numero", 5))
        self.assertFalse(self.completa(None, 5))

    def test_requerida_cero_con_algo_recibido_es_completa(self):
        # Caso borde (no deberia darse en la practica: una solicitud
        # siempre pide > 0), pero la funcion no debe reventar.
        self.assertTrue(self.completa(1, 0))


class TestRepstockMoverRecepcionParcial(unittest.TestCase):
    """🔒 2026-09-26 (revision #2, hallazgo ALTA #1): _repstock_mover con
    motivo_tipo='recepcion_parcial' -- el motivo_tipo PROPIO de las
    entregas parciales de una Compra, elegido a proposito FUERA del set
    idempotente ('recepcion_proveedor'/'instalacion') para que cada
    entrega real inserte su propio movimiento de kardex, en vez de que la
    segunda entrega se descarte como "ya existia" (que es justamente lo
    que rompia entregas parciales multiples si se reusaba
    'recepcion_proveedor')."""

    @classmethod
    def setUpClass(cls):
        cls.mover = staticmethod(_cargar_funcion("_repstock_mover"))

    def test_recepcion_parcial_no_es_idempotente_no_se_salta_el_select(self):
        # A diferencia de 'recepcion_proveedor' (ver test_repuestos_kardex.
        # test_idempotencia_recepcion_proveedor_no_duplica, que corta en 1
        # solo execute), acá NO hay chequeo de idempotencia -- el primer
        # execute ya es el SELECT...FOR UPDATE del stock, no el SELECT de
        # "ya existia".
        cur = FakeCursor([{"cantidad": 10}])
        r = self.mover(3, 3, "entrada", "recepcion_parcial",
                        solicitud_id=42, usuario="tester", cur=cur)
        self.assertTrue(r["movido"])
        self.assertFalse(r["ya_existia"])
        self.assertIn("FOR UPDATE", cur.executed[0][0])

    def test_dos_entregas_parciales_seguidas_suman_el_saldo_fisico(self):
        # Simula dos entregas reales de la MISMA solicitud/compra: la
        # primera entrega 3, la segunda entrega 2 -- cada una es su propia
        # llamada (su propio SELECT...FOR UPDATE fresco, como en
        # produccion), y el saldo fisico debe terminar en 10+3+2=15, NUNCA
        # descartar la segunda por "ya existia".
        cur1 = FakeCursor([{"cantidad": 10}])
        r1 = self.mover(3, 3, "entrada", "recepcion_parcial",
                         solicitud_id=42, usuario="tester", cur=cur1)
        self.assertTrue(r1["movido"])
        self.assertEqual(r1["saldo"], 13)

        cur2 = FakeCursor([{"cantidad": 13}])  # la 2da entrega parte del saldo YA actualizado
        r2 = self.mover(3, 2, "entrada", "recepcion_parcial",
                         solicitud_id=42, usuario="tester", cur=cur2)
        self.assertTrue(r2["movido"])
        self.assertFalse(r2["ya_existia"], "la segunda entrega NO debe descartarse como duplicada")
        self.assertEqual(r2["saldo"], 15)
        # Cada entrega insertó su PROPIO movimiento (3 executes: SELECT FOR
        # UPDATE, UPDATE stock, INSERT movimiento -- nunca 1 solo como la
        # rama idempotente).
        self.assertEqual(len(cur2.executed), 3)

    def test_nota_de_excedente_llega_intacta_al_movimiento(self):
        # El texto de "excedente recibido" lo arma repstock_compra_recibir
        # (ver TestRepstockCompraRecibirFuente más abajo) -- acá solo se
        # confirma que _repstock_mover no trunca ni descarta esa nota al
        # guardar el movimiento.
        cur = FakeCursor([{"cantidad": 5}])
        nota = "Solicitud #7 (Correa) recibida -- entrega de 4 para la compra #1. Incluye excedente recibido: 1 sobre lo pedido."
        r = self.mover(9, 4, "entrada", "recepcion_parcial",
                        solicitud_id=7, nota=nota, usuario="tester", cur=cur)
        self.assertTrue(r["movido"])
        self.assertIn("Incluye excedente recibido", cur.executed[-1][1][8])


class TestOtrepCambiarEstadoEntradaYaRegistrada(unittest.TestCase):
    """🔒 2026-09-26 (revision #2, hallazgo ALTA #1): _otrep_cambiar_estado
    NO es extraíble/ejecutable de forma aislada (usa get_db(), mysql_
    fetchone, request, _repstock_mover, etc. de app.py), asi que se revisa
    por TEXTO FUENTE -- mismo criterio que test_repuestos_kardex.py -- que
    el flag `_entrada_ya_registrada` (que pone repstock_compra_recibir
    ANTES de llamar acá, tras registrar la entrada de ESTA entrega) evita
    que este núcleo vuelva a sumar el mismo stock una segunda vez, y que
    el guard de 'instalado' reconoce 'recepcion_parcial' como una entrada
    real (si no, un repuesto recibido por entregas parciales nunca podría
    instalarse -- quedaría bloqueado creyendo que nunca tuvo entrada)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("_otrep_cambiar_estado")

    def test_flag_entrada_ya_registrada_presente(self):
        self.assertIn("_entrada_ya_registrada", self.fuente)

    def test_guard_instalado_reconoce_recepcion_parcial_como_entrada_real(self):
        self.assertIn("recepcion_parcial", self.fuente)
        # El SELECT que decide "_tiene_entrada" debe buscar AMBOS motivos.
        self.assertIn("'recepcion_proveedor','recepcion_parcial'", self.fuente.replace(" ", ""))

    def test_rechazado_y_reabrir_reinician_cantidad_recibida(self):
        # ALTA #1: "reinicia cantidad_recibida=0 al... rechazar/reabrir".
        # Se cuentan >=2 apariciones de la asignación (una en el branch
        # 'rechazado', otra en 'solicitado'/reabrir) para no depender del
        # orden exacto de las líneas.
        self.assertGreaterEqual(self.fuente.count("cantidad_recibida=0"), 2)


class TestRepstockCompraRecibirFuente(unittest.TestCase):
    """repstock_compra_recibir tampoco es aislable (endpoint Flask real) --
    revision por texto fuente de los hallazgos ALTA #1 / MEDIA #2 / #7."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("repstock_compra_recibir")

    def test_incremento_atomico_con_condicion_de_dueno_y_estado(self):
        # MEDIA #2: SET cantidad_recibida=cantidad_recibida+%s ... AND
        # compra_id=%s AND estado='pedido' -- incremento de verdad (no
        # lee-y-escribe en dos pasos separados) + candado de concurrencia.
        plano = self.fuente.replace(' ', '').replace('\n', '')
        self.assertIn("cantidad_recibida=cantidad_recibida+%s", plano)
        self.assertIn("compra_id=%sANDestado='pedido'", plano)
        # El rowcount de ESE update se revisa (no se asume éxito).
        self.assertIn("cur.rowcount", self.fuente)

    def test_registra_entrada_de_kardex_con_motivo_propio(self):
        self.assertIn("recepcion_parcial", self.fuente)
        self.assertIn("_repstock_mover(", self.fuente)

    def test_pasa_flag_entrada_ya_registrada_al_completar(self):
        # 🔒 ast.unparse normaliza SIEMPRE a comillas simples (aunque el
        # fuente en app.py use comillas dobles) -- se compara sin comillas
        # alrededor de la clave para no depender de ese detalle de estilo.
        self.assertIn("_entrada_ya_registrada': entrada_registrada", self.fuente)

    def test_excedente_se_registra_en_la_nota(self):
        self.assertIn("excedente", self.fuente.lower())

    def test_propaga_aviso_del_kardex(self):
        # MEDIA #7: "propagar payload['aviso'] a avisos".
        self.assertIn("payload.get('aviso')", self.fuente)


class TestCompraSincronizarFuente(unittest.TestCase):
    """_otrep_compra_sincronizar: revision por texto de MEDIA #4
    (concurrencia + cancelar cuando no quedan lineas vivas)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("_otrep_compra_sincronizar")

    def test_update_con_condicion_de_estado(self):
        plano = self.fuente.replace(' ', '')
        self.assertIn("WHEREid=%sANDestado=%s", plano)

    def test_cancela_si_no_quedan_lineas_no_rechazadas(self):
        # 🔒 ast.unparse normaliza a comillas simples -- ver nota en
        # TestRepstockCompraRecibirFuente.test_pasa_flag_entrada_ya_registrada_al_completar.
        self.assertIn("'cancelada'", self.fuente)
        self.assertIn("total_no_rechazadas==0", self.fuente.replace(" ", ""))

    def test_cierre_de_ticket_condicionado_al_rowcount(self):
        self.assertIn("tocada==1", self.fuente.replace(" ", ""))


class TestCrearCompraReversionFuente(unittest.TestCase):
    """_otrep_crear_compra: revision por texto de MEDIA #6 (reversion en
    try/except + finally) y ALTA #1 (reinicia cantidad_recibida en la
    reversion)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("_otrep_crear_compra")

    def test_abortar_todo_corre_en_finally(self):
        self.assertIn("finally", self.fuente)
        self.assertIn("_abortar_todo(", self.fuente)

    def test_abortar_todo_reinicia_cantidad_recibida(self):
        self.assertIn("cantidad_recibida=0", self.fuente)

    def test_revertir_pedido_exige_estado_y_dueno(self):
        plano = self.fuente.replace(" ", "")
        self.assertIn("estado='pedido'ANDcompra_id=%s", plano)


class TestCompraEstadoCancelarFuente(unittest.TestCase):
    """repstock_compra_estado: revision por texto de ALTA #1 (cancelar
    reinicia cantidad_recibida)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("repstock_compra_estado")

    def test_cancelar_reinicia_cantidad_recibida(self):
        self.assertIn("cantidad_recibida=0", self.fuente)


if __name__ == "__main__":
    unittest.main()

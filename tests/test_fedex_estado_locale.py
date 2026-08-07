"""Pruebas de _fedex_estado_a_ilus() — el traductor de estado FedEx -> ILUS.

EL BUG QUE ESTO PREVIENE (diagnosticado 2026-08-07, corregido en esta sesión)
------------------------------------------------------------------------------
La llamada a la FedEx Track API manda a propósito el header `X-locale: es_CL`
(para que `latestStatusDetail.statusByLocale` venga en español y se le pueda
mostrar tal cual al cliente en el modal de seguimiento — ver _fedex_track_lookup).

El problema real: cuando el código corto de FedEx (`latestStatusDetail.code`)
NO estaba en `_FEDEX_STATUS_MAP` (la tabla no es exhaustiva — FedEx documenta
~40 códigos y ahí solo había ~19), la función caía a un fallback que comparaba
la descripción contra palabras en INGLÉS ("delivered", "exception", "delivery
refused"...). Como esa descripción llega en ESPAÑOL por el `X-locale: es_CL`,
esas comparaciones NUNCA hacían match, y todo lo no catalogado quedaba
SIEMPRE en el default 'En ruta' — en silencio, sin loguear nada. Un código de
excepción/rechazo que FedEx use y que ILUS todavía no haya visto se mostraba
igual que un pedido normal en camino.

Los códigos DE/SE (excepción de entrega) sí estaban en el mapa, así que una
excepción "de manual" no caía en este bug particular — pero cualquier código
nuevo sí. El caso más directo para probarlo: una descripción en español real
tipo "Entregado" con un código NO mapeado -- antes de este fix, como el
fallback solo reconocía "delivered" en inglés, esto devolvía 'En ruta' en vez
de 'Entregado'. Ese es exactamente el escenario que TestFallbackEnEspanol
cubre abajo.

Fix aplicado (ver el docstring de _fedex_estado_a_ilus en app.py para el
detalle completo):
  1. `status_code` sigue siendo la fuente primaria (locale-independiente).
  2. Se agrega `derived_code` (latestStatusDetail.derivedCode) como segunda
     fuente, también locale-independiente, que antes se leía del payload
     pero nunca se usaba para decidir el estado.
  3. El fallback por texto ahora reconoce palabras en ESPAÑOL (las reales
     que llegan con es_CL), no solo en inglés.
  4. Un código sin mapear que además no matchea ningún texto queda logueado
     antes de caer al default, en vez de fallar en silencio.

No se importa app.py directo (a nivel de módulo abre conexiones a BD). Se
extrae la función + la tabla `_FEDEX_STATUS_MAP` reales con `ast` — mismo
patrón que tests/test_simpliroute_normalizar.py: se prueba el código que de
verdad se despliega, no una copia a mano.

Correr con:  py -m unittest tests.test_fedex_estado_locale -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _extraer_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


def _extraer_asignacion_modulo(nombre):
    """Busca `nombre = {...}` entre las asignaciones de NIVEL MÓDULO (no
    dentro de una función), que es donde vive _FEDEX_STATUS_MAP."""
    for nodo in _ARBOL.body:
        if isinstance(nodo, ast.Assign):
            for t in nodo.targets:
                if isinstance(t, ast.Name) and t.id == nombre:
                    return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la asignación '{nombre}' en app.py")


_NS = {}
exec(_extraer_asignacion_modulo("_FEDEX_STATUS_MAP"), _NS)
exec(_extraer_funcion("_fedex_estado_a_ilus"), _NS)
estado = _NS["_fedex_estado_a_ilus"]
MAPA = _NS["_FEDEX_STATUS_MAP"]


class TestCodigoCortoEsLaFuenteDeVerdad(unittest.TestCase):
    """El camino principal (código corto -> _FEDEX_STATUS_MAP) es
    locale-independiente por diseño: nunca dependió del texto, así que
    nunca tuvo el bug. Se prueba igual como red de seguridad — si algún día
    alguien reordena la función y la descripción pasa a decidir antes que
    el código, esto debe reventar."""

    def test_dl_delivered_con_descripcion_en_espanol(self):
        # Caso real: FedEx entrega el paquete y statusByLocale trae "Entregado"
        # (por el X-locale: es_CL). El código DL ya alcanza para clasificar.
        self.assertEqual(estado("DL", "Entregado"), "Entregado")

    def test_it_in_transit_con_descripcion_en_espanol(self):
        self.assertEqual(estado("IT", "En tránsito"), "En ruta")

    def test_de_delivery_exception_con_descripcion_en_espanol(self):
        # El caso concreto que preocupaba a Daniel: una excepción de entrega
        # real. Código DE ya está en el mapa -> nunca dependió del fallback
        # roto, por eso este caso puntual NUNCA mostró "En ruta" al cliente.
        self.assertEqual(estado("DE", "Excepción de entrega"), "Entrega fallida")

    def test_se_shipment_exception(self):
        self.assertEqual(estado("SE", "Excepción de envío"), "Entrega fallida")

    def test_codigo_case_insensitive_y_con_espacios(self):
        self.assertEqual(estado(" dl ", "Entregado"), "Entregado")


class TestFallbackEnEspanol(unittest.TestCase):
    """EL CASO DEL BUG: código NO catalogado en _FEDEX_STATUS_MAP (simula un
    código real de FedEx que ILUS todavía no vio) + descripción en español
    (lo que de verdad llega con X-locale: es_CL). Antes del fix, el fallback
    solo reconocía inglés y esto SIEMPRE caía en 'En ruta', sin importar lo
    que dijera el texto -- incluyendo rechazos y excepciones."""

    def test_entregado_en_espanol_sin_codigo_mapeado(self):
        # Antes del fix: "delivered" no está en "entregado" -> caía a 'En
        # ruta' aunque el paquete YA estaba entregado. Con el fix: matchea
        # la palabra en español.
        self.assertEqual(estado("ZZ", "Entregado en la recepción"), "Entregado")

    def test_rechazo_en_espanol_sin_codigo_mapeado(self):
        # El escenario más grave del reporte de Daniel: un rechazo/excepción
        # que la app mostraba como "va en camino".
        self.assertEqual(estado("ZZ", "Excepción de entrega - Rechazado por el destinatario"),
                          "Entrega fallida")

    def test_domicilio_cerrado_sin_codigo_mapeado(self):
        self.assertEqual(estado("ZZ", "No se pudo entregar - domicilio cerrado"),
                          "Entrega fallida")

    def test_en_transito_en_espanol_sin_codigo_mapeado(self):
        self.assertEqual(estado("ZZ", "El paquete salió de la instalación de FedEx"),
                          "En ruta")

    def test_recolectado_en_espanol_sin_codigo_mapeado(self):
        self.assertEqual(estado("ZZ", "Recolectado"), "Entregado a transporte")


class TestFallbackEnInglesCompat(unittest.TestCase):
    """Compatibilidad hacia atrás: si algún caller futuro pasa texto en
    inglés (sin pasar por X-locale=es_CL), el fallback lo sigue reconociendo
    -- no se rompió nada de lo que ya funcionaba."""

    def test_delivered_en_ingles(self):
        self.assertEqual(estado("ZZ", "Delivered"), "Entregado")

    def test_exception_en_ingles(self):
        self.assertEqual(estado("ZZ", "Delivery Exception"), "Entrega fallida")

    def test_in_transit_en_ingles(self):
        self.assertEqual(estado("ZZ", "In Transit"), "En ruta")


class TestDerivedCodeComoSegundaFuente(unittest.TestCase):
    """derivedCode es OTRO código corto locale-independiente que FedEx manda
    en el mismo bloque. Antes del fix se leía del payload pero NUNCA se usaba
    para clasificar (solo como último recurso para el texto mostrado). Ahora
    es la segunda fuente de verdad, antes de caer al texto libre."""

    def test_status_code_vacio_pero_derived_code_conocido(self):
        self.assertEqual(estado("", "algo raro sin sentido", "DL"), "Entregado")

    def test_status_code_desconocido_pero_derived_code_conocido(self):
        self.assertEqual(estado("ZZ", "algo raro sin sentido", "DE"), "Entrega fallida")

    def test_status_code_manda_sobre_derived_code(self):
        # Si AMBOS son códigos válidos y distintos, gana status_code (fuente
        # primaria) -- no se pelean, pero el orden debe ser predecible.
        self.assertEqual(estado("DL", "Entregado", "IT"), "Entregado")


class TestDefaultSeguroSinInventar(unittest.TestCase):
    """Cuando de verdad no hay nada con qué clasificar (ni código, ni
    derivedCode, ni texto reconocible), el default sigue siendo 'En ruta' --
    eso no cambió, y no debe reventar aunque venga todo vacío."""

    def test_todo_vacio(self):
        self.assertEqual(estado("", ""), "En ruta")

    def test_codigo_desconocido_y_texto_no_reconocible(self):
        self.assertEqual(estado("ZZ", "mensaje que no calza con nada conocido"),
                          "En ruta")

    def test_none_no_revienta(self):
        self.assertEqual(estado(None, None, None), "En ruta")


class TestUnFalloNuncaSeLeeComoEntregado(unittest.TestCase):
    """EL error más caro de esta función, encontrado al revisar el fix antes
    de integrarlo (2026-08-07).

    Varias descripciones de FALLO en español contienen la palabra "entregado"
    ("No entregado - destinatario ausente"). Si el chequeo de "entregado" va
    ANTES que el de los fallos, un envío que NO llegó se marca como Entregado:
    el estado queda mentido en el Monitor, se cierra el despacho, y al cliente
    le sale el correo de "tu pedido llegó" por algo que nunca recibió.

    Por eso los fallos se evalúan primero. Estas pruebas fijan ese orden.
    """

    FRASES_DE_FALLO = [
        "No entregado - destinatario ausente",
        "No entregado",
        "Excepción de entrega - Rechazado por el destinatario",
        "No se pudo entregar: domicilio cerrado",
        "Entrega rechazada por el destinatario",
        "Not delivered - recipient unavailable",
        "Undeliverable as addressed",
    ]

    def test_ninguna_frase_de_fallo_se_lee_como_entregado(self):
        for frase in self.FRASES_DE_FALLO:
            with self.subTest(frase):
                # Código desconocido -> obliga a pasar por el fallback de texto,
                # que es donde vivía el riesgo.
                r = estado("ZZ", frase)
                self.assertNotEqual(
                    r, "Entregado",
                    f"'{frase}' se leyó como ENTREGADO: el cliente recibiría el "
                    f"correo de 'tu pedido llegó' por un envío que falló")
                self.assertEqual(r, "Entrega fallida")

    def test_una_entrega_de_verdad_sigue_siendo_entregada(self):
        # El contrapeso: endurecer los fallos no puede romper el caso bueno.
        for frase in ("Entregado", "Entregado en recepción",
                      "Delivered", "Delivered to front desk"):
            with self.subTest(frase):
                self.assertEqual(estado("ZZ", frase), "Entregado")


class TestTablaDeCodigosNoSeRompio(unittest.TestCase):
    """Casos históricos reales del mapa -- que el fix no haya reordenado ni
    pisado ninguna entrada existente."""

    def test_todos_los_codigos_del_mapa_se_resuelven_directo(self):
        for code, esperado in MAPA.items():
            with self.subTest(code=code):
                # Pasando una descripción vacía: si el código resuelve solo,
                # nunca debería llegar al fallback.
                self.assertEqual(estado(code, ""), esperado)

    def test_cantidad_de_codigos_mapeados_no_bajo(self):
        # Red de seguridad floja: si alguien borra entradas del mapa por
        # error, esto avisa (no fija un número exacto para no ser frágil).
        self.assertGreaterEqual(len(MAPA), 19)


if __name__ == "__main__":
    unittest.main(verbosity=2)

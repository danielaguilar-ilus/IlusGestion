"""tr_cubicador_enviar_manifiesto ("Enviar al manifiesto" desde /asignar)
bloqueaba en seco cualquier documento con 2+ ramos pendientes (ej. despacho
+ instalación), con un 409 que mandaba a usar "Líneas pendientes".

BUG REAL EN PRODUCCIÓN (2026-08-12). Captura de Daniel, documento FCV
11216 (despacho + instalación, Ñuñoa, Transporte Felca): "Error al enviar:
El documento FCV 11216 tiene 2 ramos sin asignar (despacho, instalación)
y no pueden ir todos al mismo manifiesto. Usa 'Líneas pendientes'...".
Daniel: "según la imagen necesito modifica esa regla y que avance".

POR QUÉ EL BLOQUEO YA NO TENÍA SENTIDO: la propia corrección del
2026-08-01 (Daniel, en vivo: "me equivoqué, de hecho despachamos e
instalamos [juntos]") ya había retirado el candado que impedía que 2
ramos convivieran en el MISMO manifiesto -- lo único que quedaba en pie
era la ambigüedad de "el operador no me dijo cuál ramo quiere" en los
write-sites de documento completo (sin granularidad de línea). Pero en la
operación real, cuando hay 2 ramos pendientes casi siempre se quieren
LOS DOS en el mismo despacho -- así que en vez de forzar al operador a
usar otra pantalla, ahora se insertan todos los ramos pendientes en el
manifiesto destino.

_tr_plan_ramo_manifest_item (la función PURA que decide la acción) NO se
tocó -- sigue devolviendo "conflicto_multi_ramo" tal cual, con su misma
firma y sus mismos otros 3 usos (líneas pendientes, panel drag&drop,
asignar-a-manifiesto). El cambio es SOLO en cómo tr_cubicador_enviar_manifiesto
reacciona a esa respuesta.

Correr con:  py -m unittest tests.test_cubicador_multiramo_avanza -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class TestElConflictoYaNoBloquea(unittest.TestCase):

    def test_ya_no_hay_409_para_conflicto_multi_ramo(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        i = f.find('conflicto_multi_ramo')
        self.assertGreater(i, 0)
        bloque = f[i:i + 100]
        self.assertNotIn("_tr_resp_conflicto_multi_ramo", bloque)
        self.assertNotIn("409", bloque)

    def test_inserta_todos_los_ramos_pendientes(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        i = f.find('conflicto_multi_ramo')
        bloque = f[i:i + 700]
        self.assertIn("for _ramo_pend in _plan_cub['pendientes']", bloque)
        self.assertIn("INSERT IGNORE INTO transport_manifest_items", bloque)

    def test_no_toca_la_funcion_pura_compartida(self):
        """_tr_plan_ramo_manifest_item la usan otros 3 write-sites -- el
        fix debe quedar contenido en el caller, no en la función
        compartida, para no cambiarles el comportamiento sin querer."""
        f = _fn("_tr_plan_ramo_manifest_item")
        self.assertIn("'accion': 'conflicto_multi_ramo'", f)


class TestNotificacionCubreTodosLosRamos(unittest.TestCase):

    def test_el_evento_de_preparacion_recorre_los_items_nuevos(self):
        """Antes notificaba UN item fijo (_new_item_id). Con 2 ramos
        insertados hace falta un evento por cada uno -- el anti-spam por
        commitment de _tr_event evita el correo duplicado al cliente."""
        f = _fn("tr_cubicador_enviar_manifiesto")
        i = f.find("_tr_event(_item_id_prep")
        self.assertGreater(i, 0, "no se encontro la llamada a _tr_event dentro del loop de notificacion")
        bloque = f[max(0, i - 300):i]
        self.assertIn("for _item_id_prep, _ramo_prep in _nuevos_items_cub", bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

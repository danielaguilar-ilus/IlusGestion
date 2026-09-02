"""tr_quitar_item() bloqueaba (403, solo superadmin) quitar una factura que
NO tenía ningún tracking propio, con tal de que OTRA factura del MISMO
manifiesto ya estuviera en gestión con el courier.

Pedido de Daniel (2026-08-27, mismo día del PR de facturas duplicadas):
"mira ella quiere también eliminar los que no tengan gestión con el
transporte" -- Alison, con el permiso tr_eliminar, no podía quitar una
factura sin tracking simplemente porque el manifiesto (como conjunto)
tenía actividad en otro documento.

CAUSA REAL (confirmada leyendo el código): tr_quitar_item() abría con
_tr_manifiesto_guard_actividad(mid), que consulta si el MANIFIESTO tiene
actividad en CUALQUIERA de sus filas (transport_manifest_items.tracking_number
IS NOT NULL OR simpliroute_visit_id IS NOT NULL, o hay prueba de entrega
para cualquier commitment_id del manifiesto) -- no mira el item puntual que
se está quitando. Si tiene_actividad, exige superadmin SIN excepción, antes
de que el resto de la función (que sí es por-item) llegue a evaluarse.
Esto también tapaba por completo la ruta de "quitar factura duplicada" del
PR anterior (#189/#190): tr_eliminar nunca alcanzaba a evaluarse.

EL FIX: se quita esa llamada de tr_quitar_item() (se deja intacta en
tr_agregar_item/tr_asignar_a_manifiesto/tr_lineas_pendientes_enviar_manifiesto,
que protegen un riesgo distinto: agregar documentos a un manifiesto ya
despachado). El riesgo real que protegía -- desincronizar el seguimiento
que ve el cliente -- ya lo cubren, por item, los candados que siguen más
abajo en la misma función:
  1. Estado terminal (Entregado/Devolución) -> bloqueado sin excepción.
  2. Tracking propio + SIN copia en otro manifiesto -> bloqueado sin excepción.
  3. Tracking propio + copia en otro manifiesto -> tr_eliminar/superadmin
     + confirmación (PR #189/#190).
  4. Sin tracking propio -> libre, sin candado (igual que el botón ya
     habilitado del front para items sin _en_gestion_courier).

app.py tiene 90k+ líneas -- se extrae el cuerpo de tr_quitar_item() por
slicing de texto, mismo patrón que el resto de los tests de este módulo.

Correr con:  py -m unittest tests.test_manifiesto_quitar_item_sin_gestion -v
"""
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _fuente_tr_quitar_item():
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index("\ndef tr_quitar_item(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


SRC = _fuente_tr_quitar_item()

# La funcion arranca con un comentario largo que EXPLICA el fix -- y por
# eso menciona a proposito el nombre de la llamada vieja
# (_tr_manifiesto_guard_actividad) y la palabra "superadmin" en prosa. Buscar
# esas palabras en SRC crudo da falso positivo (estan en el comentario, no en
# codigo ejecutable). SRC_SIN_COMENTARIOS filtra las lineas que son
# comentario puro (mismo patron ya usado en test_no_llama_a_cancelar... de
# test_manifiesto_quitar_factura_duplicada.py para el mismo tipo de problema).
SRC_SIN_COMENTARIOS = "\n".join(
    l for l in SRC.split("\n") if not l.strip().startswith("#")
)


def _fuente_funcion(nombre):
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index(f"\ndef {nombre}(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


class TestElGuardDeManifiestoCompletoYaNoAbreLaFuncion(unittest.TestCase):
    """El bug real: el guard de actividad del MANIFIESTO ENTERO se
    evaluaba antes que cualquier chequeo por-item."""

    def test_ya_no_llama_al_guard_de_actividad_del_manifiesto(self):
        self.assertNotIn("_tr_manifiesto_guard_actividad(mid)", SRC_SIN_COMENTARIOS)

    def test_aviso_sigue_declarada_para_no_romper_avisos_finales(self):
        # El resto de la funcion (mas abajo) arma resp["aviso"] a partir de
        # (_aviso, _aviso_duplicado) -- _aviso debe seguir existiendo, ahora
        # simplemente None (ya no viene de un guard con bypass de superadmin).
        self.assertIn("_aviso = None", SRC_SIN_COMENTARIOS)

    def test_los_avisos_finales_siguen_funcionando(self):
        self.assertIn('_avisos_finales = [a for a in (_aviso, _aviso_duplicado) if a]', SRC)


class TestElRiesgoRealSigueCubiertoPorItem(unittest.TestCase):
    """Quitar el guard de "todo el manifiesto" no debe dejar un hueco --
    los 4 casos reales se siguen cubriendo, ahora por item puntual."""

    def test_estado_terminal_sigue_bloqueado_sin_excepcion(self):
        self.assertIn("ESTADOS_ENTREGA_TERMINALES", SRC)
        i = SRC.index("if _estado_actual in ESTADOS_ENTREGA_TERMINALES:")
        fragmento = SRC[i:i + 300]
        self.assertIn('"ok": False', fragmento)

    def test_tracking_propio_sin_permiso_sigue_bloqueado_sin_excepcion(self):
        # AMPLIADO 2026-09-02 (Daniel: "la única restricción es que tenga
        # movimiento con el courier"): ya no se exige encontrar una copia
        # duplicada -- el único candado real que sigue sin excepción es el
        # permiso. Sin tr_eliminar/superadmin, un item con tracking propio
        # se bloquea con 403 apenas se detecta el tracking, ANTES de buscar
        # ninguna duplicada.
        i_tracking = SRC.index('if info and (info.get("master_tracking_number")')
        i_permiso = SRC.index(
            'if not (g.permissions.get("superadmin") or g.permissions.get("tr_eliminar")):',
            i_tracking)
        i_duplicada_query = SRC.index("duplicada = mysql_fetchone(", i_tracking)
        self.assertLess(i_tracking, i_permiso,
            "el chequeo de permiso debe estar dentro del bloque de tracking propio")
        self.assertLess(i_permiso, i_duplicada_query,
            "el permiso se exige ANTES de buscar una duplicada, no despues")

    def test_tracking_propio_con_duplicada_exige_tr_eliminar(self):
        self.assertIn('g.permissions.get("superadmin") or g.permissions.get("tr_eliminar")', SRC)

    def test_sin_tracking_propio_no_encuentra_ningun_candado(self):
        # Si el item no tiene master_tracking_number/tracking_number/
        # simpliroute_visit_id, el "if info and (...)" que envuelve TODA la
        # logica de candados de tracking simplemente no se ejecuta -- no debe
        # quedar, antes de ese if, ningun return 403 que dependa de superadmin
        # sin importar el item.
        i_inicio = SRC_SIN_COMENTARIOS.index("def tr_quitar_item(mid, item_id):")
        i_primer_if_tracking = SRC_SIN_COMENTARIOS.index(
            'if info and (info.get("master_tracking_number")')
        fragmento_previo = SRC_SIN_COMENTARIOS[i_inicio:i_primer_if_tracking]
        self.assertNotIn("superadmin", fragmento_previo)
        self.assertNotIn("403", fragmento_previo)


class TestLaUnicaRestriccionEsTenerMovimientoConElCourier(unittest.TestCase):
    """Caso real reportado por Daniel (2026-09-02): Alison seguía sin poder
    quitar facturas duplicadas. Causa: tr_quitar_item() exigía ADEMÁS
    encontrar una copia VIVA de la misma factura en otro manifiesto no
    eliminado -- si esa copia ya no calificaba (su manifiesto también se
    había limpiado, o nunca quedó una segunda fila calzando el criterio),
    el candado bloqueaba PARA SIEMPRE sin excepción, ni para superadmin.

    Daniel, en vivo: "recuerda que la única restricción es que tenga
    movimiento con el courier". Ahora: sin movimiento -> libre (ver
    TestElRiesgoRealSigueCubiertoPorItem); con movimiento -> se puede
    quitar con tr_eliminar/superadmin + confirmación, exista o no una
    copia duplicada detectada."""

    def test_sin_duplicada_detectada_ya_no_bloquea_sin_excepcion(self):
        # El viejo "if not duplicada: return ... 409" incondicional (incluso
        # para superadmin) ya no existe -- la ausencia de una duplicada solo
        # cambia el mensaje, dentro del else de "if duplicada:".
        self.assertNotIn("if not duplicada:", SRC)

    def test_el_permiso_gobierna_independiente_de_si_hay_duplicada(self):
        i_permiso = SRC.index(
            'if not (g.permissions.get("superadmin") or g.permissions.get("tr_eliminar")):')
        i_duplicada_query = SRC.index("duplicada = mysql_fetchone(")
        # El chequeo de permiso queda ANTES de siquiera consultar si existe
        # una duplicada -- ya no depende de haberla encontrado.
        self.assertLess(i_permiso, i_duplicada_query)

    def test_sin_duplicada_arma_un_mensaje_de_confirmacion_generico(self):
        i = SRC.index("Sin otra copia calzando el criterio")
        fragmento = SRC[i:i + 500]
        self.assertIn("_msg_duplicada = (", fragmento)
        self.assertIn("¿Confirmas que quieres quitarla igual del manifiesto?", fragmento)

    def test_no_inventa_una_copia_duplicada_que_no_existe(self):
        # El JSON de confirmacion no debe mandar un correlativo/manifest_id
        # inventado cuando no se encontro duplicada -- debe ser None.
        i = SRC.index('"duplicada_manifiesto": duplicada.get("correlativo") if duplicada else None')
        self.assertGreater(i, 0)
        i2 = SRC.index('"duplicada_manifiesto_id": duplicada.get("manifest_id") if duplicada else None')
        self.assertGreater(i2, 0)

    def test_el_log_de_auditoria_distingue_el_caso_sin_duplicada_detectada(self):
        self.assertIn("QUITADA CON GESTIÓN ACTIVA", SRC)


class TestNoSeTocoElGuardDeAgregar(unittest.TestCase):
    """REGLA #4.2: el pedido es solo sobre "quitar" -- agregar/asignar a un
    manifiesto ya despachado sigue protegido igual que siempre."""

    def test_tr_agregar_item_sigue_usando_el_guard(self):
        fuente = _fuente_funcion("tr_agregar_item")
        self.assertIn("_tr_manifiesto_guard_actividad(mid)", fuente)

    def test_tr_asignar_a_manifiesto_sigue_usando_el_guard(self):
        fuente = _fuente_funcion("tr_asignar_a_manifiesto")
        self.assertIn("_tr_manifiesto_guard_actividad(mid)", fuente)

    def test_tr_lineas_pendientes_enviar_manifiesto_sigue_usando_el_guard(self):
        fuente = _fuente_funcion("tr_lineas_pendientes_enviar_manifiesto")
        self.assertIn("_tr_manifiesto_guard_actividad(mid)", fuente)

    def test_la_funcion_guard_compartida_sigue_existiendo(self):
        # No se borra la funcion compartida -- solo se deja de llamar desde
        # tr_quitar_item. Sigue sirviendo a las 3 rutas de arriba.
        fuente = _fuente_funcion("_tr_manifiesto_guard_actividad")
        self.assertIn("tiene_actividad", fuente)


if __name__ == "__main__":
    unittest.main(verbosity=2)

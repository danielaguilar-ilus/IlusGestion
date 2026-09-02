"""Quitar una factura duplicada de un manifiesto (Alison, sin depender de un
superadmin) -- regla acordada con Daniel el 22-08-2026 (ver memoria
etiqueta-desfasada-y-multimanifiesto), pedida de nuevo el 27-08-2026: "que
Alison pueda borrar tanto facturas que no tengan movimiento y manifiestos".

EL HUECO REAL (confirmado leyendo el código antes del fix): la misma
factura (mismo commitment_id) puede estar repetida en varios manifiestos
(badge "N despachos"). tr_quitar_item() bloqueaba SIEMPRE que la copia
tuviera tracking (master_tracking_number / tracking_number /
simpliroute_visit_id) -- sin importar si esa entrega YA pasó de verdad en
OTRO manifiesto. Caso real (BLV 23093, 3 despachos): una copia se entregó,
las otras 2 quedaban pegadas para siempre, sin forma de limpiarlas.

LA REGLA (22-08-2026, "vamos afinando"):
  1. Si hay una copia con estado_entrega='Entregado' en OTRO manifiesto no
     eliminado -> esta es un duplicado sin movimiento real, se puede quitar.
  2. Permiso: superadmin o tr_eliminar (mismo flag granular que ya gobierna
     borrar manifiestos completos -- no se multiplican permisos por una
     variante del mismo gesto de limpieza).
  3. Confirmación previa mostrando en qué manifiesto se entregó, cuándo y
     con qué tracking (mismo patrón que tr_manifiesto_eliminar: primera
     llamada sin confirmar -> 409 con los datos; segunda llamada con
     {"confirmado": true} -> borra).
  4. La etiqueta FedEx de ESTA copia NO se anula sola -- se avisa con el
     tracking para anularla a mano si no se va a usar.
  5. Si NO hay una copia entregada en otro lado, sigue el candado de
     siempre sin excepción (REGLA #4.2: no relajar lo que no se acordó).

app.py tiene 90k+ líneas -- se extrae el cuerpo de tr_quitar_item() por
slicing de texto (mismo patrón ya usado hoy en otros tests del cubicador),
sin pagar el costo de un ast.parse() completo (~85s en este equipo).

Correr con:  py -m unittest tests.test_manifiesto_quitar_factura_duplicada -v
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

with open(os.path.join(BASE_DIR, "static", "transporte_manifiesto_detalle.js"),
          encoding="utf-8", errors="ignore") as _f:
    JS_SRC = _f.read()


def _cuerpo_quitar_item_js():
    i = JS_SRC.index("async function quitarItem(")
    j = JS_SRC.index("\n}\n", i)
    return JS_SRC[i:j + 2]


JS_QUITAR = _cuerpo_quitar_item_js()


class TestElCandadoOriginalSigueVigenteSinDuplicado(unittest.TestCase):
    """AMPLIADO 2026-09-02 (Daniel, en vivo, Alison seguía sin poder quitar
    duplicadas reales: "recuerda que la única restricción es que tenga
    movimiento con el courier"). Antes, si NO había ninguna otra copia
    calzando el criterio, el candado bloqueaba PARA SIEMPRE sin excepción,
    ni para superadmin -- eso era justo el bug real que Alison reportó (la
    otra copia dejaba de calificar y ya no se podía quitar ninguna). Ahora
    la ausencia de una copia detectada solo cambia el mensaje -- con
    tr_eliminar/superadmin, igual se puede quitar confirmando."""

    def test_sin_duplicada_el_mensaje_sigue_mencionando_gestion_con_courier(self):
        # El texto "en gestión con el courier" sigue apareciendo (ahora en
        # el mensaje de confirmación, no como bloqueo incondicional).
        self.assertIn("Esta factura ya está en gestión con el courier", SRC)

    def test_la_consulta_de_duplicada_ya_no_exige_estado_entregado(self):
        # AMPLIADO 2026-08-27 (Daniel: "no son duplicados, quiero que ella
        # pueda dejar limpio, se ensució mucho los manifiestos"): antes se
        # exigia que la otra copia estuviera 'Entregado' -- ahora alcanza con
        # que exista en otro manifiesto activo, sin importar su estado. La
        # condicion NO puede aparecer en la clausula WHERE (i < i_order).
        i_where = SRC.index("WHERE mi2.commitment_id = %s AND mi2.id != %s")
        i_order = SRC.index("ORDER BY", i_where)
        clausula_where = SRC[i_where:i_order]
        self.assertNotIn("estado_entrega", clausula_where)

    def test_pero_prioriza_mostrar_la_copia_entregada_si_existe(self):
        # Si hay una entregada Y una pendiente, se prefiere mostrar la
        # entregada (dato mas util para decidir) -- eso vive en el ORDER BY,
        # no en el WHERE.
        i_order = SRC.index("ORDER BY (mi2.estado_entrega = 'Entregado') DESC")
        self.assertGreater(i_order, 0)

    def test_la_consulta_de_duplicada_excluye_manifiestos_eliminados(self):
        self.assertIn("m2.eliminado = 0", SRC)

    def test_la_consulta_de_duplicada_excluye_la_propia_copia(self):
        self.assertIn("mi2.id != %s", SRC)


class TestPermisoParaQuitarDuplicada(unittest.TestCase):
    def test_exige_superadmin_o_tr_eliminar(self):
        i = SRC.index('if not (g.permissions.get("superadmin")')
        fragmento = SRC[i:i + 200]
        self.assertIn('g.permissions.get("superadmin")', fragmento)
        self.assertIn('g.permissions.get("tr_eliminar")', fragmento)

    def test_el_mensaje_de_permiso_denegado_dirige_a_usuarios_y_roles(self):
        # El mensaje humano es un string largo partido en varias lineas de
        # codigo (mismo estilo que el mensaje ya existente de
        # tr_manifiesto_eliminar en la linea ~30416-30418) -- Python
        # concatena literales adyacentes en tiempo de compilacion, pero el
        # texto CRUDO del archivo tiene el salto de linea Y las comillas de
        # cierre/apertura de cada literal en medio de la frase
        # ("...manifiestos y \"\n   \"pedidos\"..."). Quitar solo los saltos
        # de linea no alcanza -- las comillas sueltas quedan como token propio
        # entre "y" y "pedidos" aunque se colapsen los espacios. Se quitan
        # las comillas (escapadas o no) y RECIEN despues se normalizan los
        # espacios, antes de buscar la frase.
        texto_sin_comillas = SRC.replace('\\"', "").replace('"', "")
        src_normalizado = " ".join(texto_sin_comillas.split())
        self.assertIn("Usuarios y roles", src_normalizado)
        self.assertIn("Eliminar manifiestos y pedidos", src_normalizado)

    def test_reusa_tr_eliminar_no_crea_un_permiso_nuevo(self):
        """Pedido de Daniel: 'que Alison pueda borrar tanto facturas... y
        manifiestos' -- un solo flag gobierna las dos capacidades."""
        n_ocurrencias = SRC.count('"tr_eliminar"')
        self.assertGreaterEqual(n_ocurrencias, 1)
        self.assertNotIn("tr_eliminar_duplicados", SRC)
        self.assertNotIn("tr_limpiar_duplicados", SRC)


class TestConfirmacionPreviaConLosDatosReales(unittest.TestCase):
    def test_primera_llamada_sin_confirmado_pide_confirmacion(self):
        i = SRC.index("if not body.get(\"confirmado\"):")
        fragmento = SRC[i:i + 500]
        self.assertIn('"requiere_confirmacion": True', fragmento)

    def test_la_confirmacion_incluye_manifiesto_fecha_y_tracking_reales(self):
        i = SRC.index("if not body.get(\"confirmado\"):")
        fragmento = SRC[i:i + 700]
        self.assertIn("duplicada_manifiesto", fragmento)
        self.assertIn("duplicada_fecha", fragmento)
        self.assertIn("duplicada_tracking", fragmento)

    def test_usa_chile_fmt_para_la_fecha_no_iso_crudo(self):
        # REGLA #6: ninguna fecha se muestra en formato ISO crudo.
        self.assertIn("chile_fmt_filter(_fecha_ent", SRC)


class TestFuncionaTambienConCopiaNoEntregadaTodavia(unittest.TestCase):
    """AMPLIADO 2026-08-27 (Daniel, mismo día que el pedido original: "no son
    duplicados, quiero que ella pueda dejar limpio, se ensució mucho los
    manifiestos"): la otra copia no tiene por qué estar ENTREGADA -- alcanza
    con que exista en otro manifiesto activo para poder elegir cuál dejar."""

    def test_distingue_si_la_otra_copia_ya_se_entrego_o_no(self):
        self.assertIn(
            '_ya_entregada_en_otro = bool(duplicada and duplicada.get("estado_entrega") == "Entregado")',
            SRC)

    def test_el_mensaje_no_miente_diciendo_entregada_si_no_lo_esta(self):
        i = SRC.index("else:")
        i_msg = SRC.index("_msg_duplicada = (f\"Esta factura también está", i)
        self.assertGreater(i_msg, 0, "falta el mensaje para el caso 'repetida pero no entregada'")
        fragmento = SRC[i_msg:i_msg + 300]
        self.assertIn("Son dos copias del mismo documento", fragmento)

    def test_la_respuesta_incluye_el_estado_real_de_la_otra_copia(self):
        i = SRC.index("if not body.get(\"confirmado\"):")
        fragmento = SRC[i:i + 800]
        self.assertIn("duplicada_estado", fragmento)
        self.assertIn("duplicada_ya_entregada", fragmento)

    def test_la_fecha_queda_vacia_cuando_no_esta_entregada(self):
        # No se inventa una fecha de entrega que no existe (REGLA #6 al reves:
        # mejor nada que un dato falso).
        i = SRC.index('"duplicada_fecha": _fecha_txt if _ya_entregada_en_otro else')
        self.assertGreater(i, 0)


class TestElFedexNoSeAnulaSolo(unittest.TestCase):
    """REGLA #4.2 aplicada a un sistema externo: 'nada irreversible sin
    decisión humana' -- quitar del manifiesto ILUS no debe cancelar nada en
    FedEx por su cuenta."""

    def test_no_llama_a_cancelar_ni_anular_el_tracking_propio(self):
        # El mensaje humano SI menciona "cancela" (para pedirle a la persona
        # que lo haga a mano) -- lo que no debe existir es una LLAMADA que
        # dispare la cancelacion sola (una peticion HTTP a FedEx, o una
        # funcion tipo _fedex_cancelar/_fedex_anular).
        i = SRC.index("_tracking_propio = info.get(")
        fragmento = SRC[i:i + 400]
        self.assertNotIn("_fedex_cancelar", fragmento)
        self.assertNotIn("_fedex_anular", fragmento)
        self.assertNotIn("requests.", fragmento)
        self.assertIn("anúlalo manualmente", fragmento)
        self.assertIn("no se cancela automáticamente", fragmento)

    def test_el_aviso_llega_al_frontend_en_la_respuesta(self):
        self.assertIn('resp["aviso"]', SRC)
        self.assertIn("_avisos_finales", SRC)


class TestElLogDeAuditoriaMarcaElCasoDuplicado(unittest.TestCase):
    def test_la_trazabilidad_distingue_una_copia_duplicada(self):
        self.assertIn("COPIA DUPLICADA", SRC)
        i = SRC.index("COPIA DUPLICADA")
        # Debe estar ANTES de _tr_log (Regla #5: log antes de borrar, no despues).
        i_log = SRC.index('_tr_log("manifest", mid, "factura quitada"')
        self.assertLess(i, i_log)


class TestFrontendManejaElFlujoDeConfirmacionEnDosPasos(unittest.TestCase):
    def test_reintenta_con_confirmado_true_tras_aceptar_el_segundo_dialogo(self):
        self.assertIn("requiere_confirmacion", JS_QUITAR)
        self.assertIn("quitarItem(mid, itemId, true)", JS_QUITAR)

    def test_muestra_manifiesto_fecha_y_tracking_de_la_copia_real(self):
        self.assertIn("duplicada_manifiesto", JS_QUITAR)
        self.assertIn("duplicada_fecha", JS_QUITAR)
        self.assertIn("duplicada_tracking", JS_QUITAR)

    def test_muestra_el_aviso_de_fedex_si_viene_en_la_respuesta(self):
        self.assertIn("d.aviso", JS_QUITAR)

    def test_manda_content_type_json_solo_cuando_reintenta_confirmado(self):
        # El primer intento (sin confirmar) sigue siendo un DELETE simple,
        # sin body -- no romper el caso general que no toca esta rama.
        self.assertIn("confirmado ? JSON.stringify", JS_QUITAR)


if __name__ == "__main__":
    unittest.main()

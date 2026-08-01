"""
Tests de CARACTERIZACIÓN del módulo Transporte (Fase 0 del plan de mejora
integral, 2026-07-31).

QUÉ ES UN TEST DE CARACTERIZACIÓN
---------------------------------
NO afirma lo que el sistema DEBERÍA hacer. Afirma lo que el sistema HACE HOY,
incluyendo sus bugs conocidos. Sirve para dos cosas:

  1. Congelar el comportamiento actual antes de refactorizar, para que si un
     cambio de Fase 1+ lo altera SIN QUERER, el test falle y avise.
  2. Dejar por escrito y en un lugar ejecutable los gaps detectados, en vez de
     que vivan en la cabeza de alguien o en un documento que nadie lee.

Cuando una Fase futura ARREGLE uno de estos gaps a propósito, el test
correspondiente va a fallar. Eso es lo correcto y esperado: hay que actualizar
el test en el mismo PR que arregla el gap, nunca borrarlo en silencio.
Cada test marcado con [GAP] lleva escrito qué se espera que pase cuando se
arregle.

Correr:  python3 tests/test_transport_caracterizacion.py
"""
import ast
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

APP_PY = os.path.join(RAIZ, "app.py")


# ─── Utilidades: extraer una función de app.py sin importar los 80K líneas ───
_FUENTE_APP = None


def _fuente_app():
    """Lee app.py una sola vez (son varios MB)."""
    global _FUENTE_APP
    if _FUENTE_APP is None:
        with open(APP_PY, encoding="utf-8") as fh:
            _FUENTE_APP = fh.read()
    return _FUENTE_APP


_ARBOL_APP = None
_FUNCS_APP = None
_LINEAS_APP = None


def _lineas_app():
    """app.py partido en líneas, una sola vez."""
    global _LINEAS_APP
    if _LINEAS_APP is None:
        _LINEAS_APP = _fuente_app().splitlines()
    return _LINEAS_APP


def _arbol_app():
    """Parsea app.py UNA sola vez. Parsear 80K líneas cuesta ~2s: hacerlo por
    cada aserción volvía la suite inusablemente lenta."""
    global _ARBOL_APP
    if _ARBOL_APP is None:
        _ARBOL_APP = ast.parse(_fuente_app())
    return _ARBOL_APP


def _funcs_app():
    """Índice {nombre: nodo} de TODAS las funciones de app.py (un solo walk)."""
    global _FUNCS_APP
    if _FUNCS_APP is None:
        _FUNCS_APP = {}
        for nodo in ast.walk(_arbol_app()):
            if isinstance(nodo, ast.FunctionDef):
                _FUNCS_APP.setdefault(nodo.name, nodo)
    return _FUNCS_APP


def _extraer_funcion(nombre, extras=None):
    """Extrae UNA función de app.py y la ejecuta en un namespace aislado.

    Mismo patrón que tests/test_transport_label_address.py: evita importar
    app.py entero (Flask + MySQL + ERP + credenciales) solo para probar una
    función pura.
    """
    nodo = _funcs_app().get(nombre)
    if nodo is None:
        raise AssertionError(f"No se encontró la función {nombre}() en app.py")
    ns = dict(extras or {})
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), APP_PY, "exec"), ns)
    return ns[nombre]


def _cuerpo_funcion(nombre):
    """Devuelve el código fuente de una función de app.py como string.

    Para las aserciones ESTRUCTURALES (las que miran el SQL literal), donde no
    se puede ejecutar la función porque depende de MySQL/ERP.
    """
    nodo = _funcs_app().get(nombre)
    if nodo is None:
        raise AssertionError(f"No se encontró la función {nombre}() en app.py")
    # ast.get_source_segment() vuelve a partir en líneas el archivo ENTERO en
    # cada llamada (~2s cada vez sobre app.py, ~170s la suite). Cortamos sobre
    # las líneas ya cacheadas: mismo resultado, instantáneo.
    return "\n".join(_lineas_app()[nodo.lineno - 1:nodo.end_lineno])


def _norm(texto):
    """Normaliza espacios: hace las aserciones sobre SQL inmunes al indentado."""
    return re.sub(r"\s+", " ", texto)


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 1 · Clasificación de un documento según sus líneas ZZ
# ═════════════════════════════════════════════════════════════════════════════
class TestClasificacionZZ(unittest.TestCase):
    """_clasif_from_skus() decide si un documento es despacho / instalación /
    retiro / mantención. Es la pieza que hoy decide en qué "ramo" cae una
    factura, así que cualquier cambio del modelo de dos ramos (Fase 1) pasa
    por acá."""

    @classmethod
    def setUpClass(cls):
        # staticmethod: si no, acceder a self.clasif la convierte en método
        # vinculado y le pasa `self` como primer argumento.
        cls.clasif = staticmethod(_extraer_funcion("_clasif_from_skus"))

    def test_solo_zzenvio_es_despacho(self):
        self.assertEqual(self.clasif(["ZZENVIO"]), "despacho")

    def test_solo_zzinstalacion_es_instalacion(self):
        self.assertEqual(self.clasif(["ZZINSTALACION"]), "instalacion")

    def test_sin_lineas_zz_cae_a_despacho_por_defecto(self):
        self.assertEqual(self.clasif([]), "despacho")
        self.assertEqual(self.clasif([None, "", "  "]), "despacho")

    def test_solo_zzretiro_es_retiro(self):
        self.assertEqual(self.clasif(["ZZRETIRO", "ZZRETIRO"]), "retiro")

    def test_es_case_insensitive(self):
        # El ERP devuelve los SKU con mayúsculas/minúsculas inconsistentes
        # (la constante global ZZ_SKUS de app.py mezcla 'ZZenvio' con
        # 'ZZINSTALACION'), así que la normalización importa.
        self.assertEqual(self.clasif(["zzinstalacion"]), "instalacion")
        self.assertEqual(self.clasif(["  ZZreTiro  "]), "retiro")

    def test_GAP_envio_mas_instalacion_pierde_el_envio(self):
        """[GAP] Una factura con ZZENVIO **y** ZZINSTALACION queda clasificada
        SOLO como 'instalacion'. La señal de que también hay que despachar se
        pierde: no existe forma de que un documento pertenezca a los dos ramos.

        Esto es exactamente lo que el plan de dos ramos (Despacho / Instalación)
        tiene que resolver en Fase 1.

        CUANDO SE ARREGLE: este test va a fallar. Reemplazarlo por uno que
        verifique que el documento aparece en AMBOS ramos — no borrarlo.
        """
        self.assertEqual(self.clasif(["ZZENVIO", "ZZINSTALACION"]), "instalacion")

    def test_GAP_retiro_solo_gana_si_TODAS_las_lineas_son_retiro(self):
        """[GAP] 'retiro' usa all(), el resto usa any(). Un documento con
        ZZRETIRO + ZZENVIO NO es retiro: cae a 'despacho'. Documentado acá
        porque es una asimetría fácil de romper sin querer al refactorizar."""
        self.assertEqual(self.clasif(["ZZRETIRO", "ZZENVIO"]), "despacho")


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 2 · Constante ZZ_SKUS duplicada en 3 lugares
# ═════════════════════════════════════════════════════════════════════════════
class TestZZSkusDuplicada(unittest.TestCase):
    def test_las_tres_copias_de_ZZ_SKUS_son_equivalentes(self):
        """app.py define el conjunto de SKUs ZZ TRES veces (una global
        ZZ_SKUS + dos locales _ZZ_SKUS). Si alguien agrega un SKU nuevo en una
        sola, el sistema empieza a comportarse distinto según qué código corra.

        Este test no exige que se unifiquen (eso es trabajo de otra fase), solo
        que mientras existan las tres, digan lo mismo.
        """
        conjuntos = []
        for nodo in ast.walk(_arbol_app()):
            if not isinstance(nodo, ast.Assign):
                continue
            for destino in nodo.targets:
                if isinstance(destino, ast.Name) and destino.id in ("ZZ_SKUS", "_ZZ_SKUS"):
                    try:
                        valor = ast.literal_eval(nodo.value)
                    except Exception:
                        continue
                    conjuntos.append({str(s).strip().upper() for s in valor})

        self.assertGreaterEqual(
            len(conjuntos), 3,
            "Se esperaban al menos 3 definiciones de ZZ_SKUS/_ZZ_SKUS; "
            f"se encontraron {len(conjuntos)}. Si se unificaron, actualizar este test.",
        )
        primera = conjuntos[0]
        for i, otra in enumerate(conjuntos[1:], start=2):
            self.assertEqual(
                primera, otra,
                f"La copia #{i} de ZZ_SKUS difiere de la primera. "
                f"Solo en la primera: {primera - otra}. Solo en la #{i}: {otra - primera}.",
            )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 3 · Sync masivo desde el ERP (_tr_bulk_sync_erp_mysql)
# ═════════════════════════════════════════════════════════════════════════════
class TestBulkSyncErp(unittest.TestCase):
    """El sync masivo corre por cron varias veces al día sobre TODOS los
    documentos de la ventana. Cualquier cosa que pise datos acá los pisa a
    escala, no en un documento suelto."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_tr_bulk_sync_erp_mysql")
        cls.sql = _norm(cls.fuente)

    def test_el_sync_masivo_nunca_reemplaza_una_guia_conocida_por_vacio(self):
        """REGRESIÓN (arreglado 2026-07-31, encontrado por estos mismos tests).

        Antes: el UPSERT hacía `guia_numero=VALUES(guia_numero)` a secas. Como
        la consulta a SQL Server no trae la guía (ver el test de NUDGIA), el
        valor era SIEMPRE NULL → **cada corrida del cron borraba la guía de
        todos los documentos de la ventana**, incluidas las que había poblado
        el sync individual o una persona a mano.

        Ahora: misma guarda que comuna/direccion/telefono/email. Si la corrida
        no trae guía, se conserva la que ya estaba.

        Este test es el que pide explícitamente el plan de Fase 0 ("garantizar
        que la sincronización masiva nunca sustituya una guía conocida por
        vacío"). Si alguien vuelve a la forma sin guarda, falla acá.
        """
        sin_espacios = self.sql.replace(" ", "")
        self.assertIn(
            "guia_numero=IF(VALUES(guia_numero)ISNULLORVALUES(guia_numero)='',guia_numero,VALUES(guia_numero))",
            sin_espacios,
            "El sync masivo perdió la guarda de guia_numero: volvió a poder "
            "borrar guías buenas al correr por cron.",
        )

    def test_el_sync_masivo_nunca_reemplaza_nombre_o_rut_por_vacio(self):
        """REGRESIÓN (arreglado 2026-07-31). Mismo agujero que el de arriba,
        en `cliente_nombre`/`cliente_rut`: se pisaban incondicionalmente.

        Es el hermano masivo del bug que ya se había arreglado en el sync
        individual (`_tr_fetch_from_erp`) el 2026-07-31 tras perder el nombre
        real de un cliente en producción. Acá el impacto es mayor porque el
        cron recorre toda la ventana, no un documento suelto.
        """
        sin_espacios = self.sql.replace(" ", "")
        # cliente_rut: guarda simple (no existe centinela para RUT).
        self.assertIn(
            "cliente_rut=IF(VALUES(cliente_rut)ISNULLORVALUES(cliente_rut)='',cliente_rut,VALUES(cliente_rut))",
            sin_espacios,
            "El sync masivo perdió la guarda de cliente_rut.",
        )
        # cliente_nombre: la guarda DEBE contemplar además el centinela
        # "Cliente no informado por ERP" — el bulk sync lo asigna cuando esta
        # pasada no resuelve nombre, y NO es vacío, así que la guarda simple
        # lo dejaba pasar y pisaba nombres buenos (revisión adversarial
        # 2026-08-01; mismo bug que ya había mordido al sync individual).
        self.assertIn("clientenoinformadoporerp",
                      sin_espacios.lower(),
                      "La guarda de cliente_nombre del sync masivo perdió el "
                      "centinela: vuelve a poder pisar nombres buenos con "
                      "'Cliente no informado por ERP'.")

    def test_GAP_NUDGIA_viene_hardcodeada_vacia_del_SQL(self):
        """[GAP CRÍTICO] La consulta a SQL Server no trae el número de guía:
        selecciona el literal `'' AS NUDGIA`. Por eso en Python
        `guia` siempre resulta None.

        CUANDO SE ARREGLE: reemplazar el literal por la columna/join real.
        """
        self.assertRegex(
            self.sql,
            r"''\s+AS\s+NUDGIA",
            "NUDGIA ya no viene hardcodeada como '' — actualizar este test.",
        )

    def test_GAP_despachado_parcial_es_rama_muerta_en_el_bulk_sync(self):
        """[GAP] Consecuencia directa de los dos tests anteriores: la rama
        `elif guia: estado_auto = "Despachado parcial"` existe en el código
        pero NUNCA se ejecuta en este flujo, porque `guia` siempre es None.

        Resultado real: todo documento con saldo cae a 'Pendiente', nunca a
        'Despachado parcial', aunque el docstring de la función diga lo
        contrario. Los tres elementos siguen presentes en el código.
        """
        self.assertIn('estado_auto = "Despachado parcial"', self.fuente)
        self.assertIn("elif guia:", self.fuente)
        self.assertRegex(
            _norm(self.fuente),
            r'guia\s*=\s*\(row\.get\("NUDGIA"\)\s*or\s*""\)\.strip\(\)\s*or\s*None',
            "Cambió la forma en que se deriva `guia` — reevaluar si la rama "
            "'Despachado parcial' dejó de ser inalcanzable.",
        )

    def test_estado_manual_no_se_pisa_si_lo_toco_un_humano(self):
        """Comportamiento CORRECTO que hay que preservar: el UPSERT solo pisa
        `estado` si el valor actual es auto-gestionado Y `updated_by='sync'`.
        Si una persona movió el documento a mano, el cron lo respeta."""
        self.assertIn("updated_by='sync'", self.sql)
        self.assertRegex(
            self.sql,
            r"estado\s*=CASE WHEN estado IN \([^)]*\) AND updated_by='sync' THEN VALUES\(estado\) ELSE estado END",
        )

    def test_saldo_cero_es_Despachado_no_Entregado(self):
        """Regresión histórica (FIX 2026-07-25): saldo ZZ == 0 significa
        'facturado/cubierto en el ERP', NO 'entregado físicamente'. Alguna vez
        este código escribió 'Entregado' y mezcló plata con logística.

        Este test existe para que ese error no vuelva.
        """
        self.assertRegex(
            _norm(self.fuente),
            r"if saldo_zz <= 0: estado_auto = \"Despachado\"",
        )
        # Y que el bulk sync NO escriba 'Entregado' como estado automático.
        estados_auto = re.findall(r'estado_auto\s*=\s*"([^"]+)"', self.fuente)
        self.assertNotIn(
            "Entregado", estados_auto,
            "El bulk sync volvió a derivar 'Entregado' desde el saldo del ERP. "
            "Eso confunde saldo financiero con entrega física (bug de 2026-07-25).",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 3.b · Sync individual (_tr_fetch_from_erp) — mismas guardas
# ═════════════════════════════════════════════════════════════════════════════
class TestSyncIndividual(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sql = _norm(_cuerpo_funcion("_tr_fetch_from_erp")).replace(" ", "")

    def test_no_pisa_guia_nombre_ni_rut_con_vacio(self):
        """El sync individual comparte los tres campos frágiles con el masivo.
        Acá el dato SÍ puede venir real (vía REST), así que la guarda no impide
        actualizarlo: solo impide que una corrida sin dato borre el que había.

        `cliente_nombre` además contempla el centinela ERP_NO_CLIENT, que no es
        cadena vacía y por eso se colaba por la primera versión de la guarda
        (bug real: se perdió el nombre de un cliente en producción).
        """
        self.assertIn(
            "guia_numero=IF(VALUES(guia_numero)ISNULLORVALUES(guia_numero)='',guia_numero,VALUES(guia_numero))",
            self.sql,
            "El sync individual volvió a pisar guia_numero sin guarda.",
        )
        self.assertIn(
            "cliente_rut=IF(VALUES(cliente_rut)ISNULLORVALUES(cliente_rut)='',cliente_rut,VALUES(cliente_rut))",
            self.sql,
            "El sync individual volvió a pisar cliente_rut sin guarda.",
        )
        self.assertIn("cliente_nombre=IF(", self.sql,
                      "El sync individual volvió a pisar cliente_nombre sin guarda.")


class TestCronRefrescoSaldo(unittest.TestCase):
    """El cron horario de saldo es el único proceso que consulta el ERP
    documento por documento. Su alcance define cuánto lo golpeamos."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_cron_refrescar_saldo_productos")

    def test_deja_de_consultar_documentos_que_ya_no_tienen_saldo(self):
        """Pedido explícito de Daniel (2026-07-31): "que sea una vez, si ya no
        tiene saldo no quiero llamarlo por siempre".

        Sin este filtro, un documento con saldo 0 se volvía a consultar cada
        hora indefinidamente, sin que el dato pudiera cambiar.
        """
        self.assertIn(
            "c.tiene_saldo = 1", self.fuente,
            "El cron perdió el filtro de saldo: volvió a consultar para siempre "
            "documentos que ya no tienen nada pendiente.",
        )

    def test_sigue_acotado_a_despachos_no_terminales(self):
        """El otro límite de alcance: nunca consulta despachos ya cerrados."""
        self.assertIn("estado_entrega NOT IN", self.fuente)
        for terminal in ("Entregado", "Entrega fallida", "Devolución"):
            self.assertIn(terminal, self.fuente)

    def test_tiene_techo_duro_de_consultas_por_corrida(self):
        """Aunque los filtros fallen, una corrida no puede disparar consultas
        ilimitadas contra el ERP."""
        self.assertRegex(
            _norm(self.fuente), r"LIMIT \d+",
            "El cron perdió su LIMIT: una corrida podría golpear el ERP sin techo.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 4 · Idempotencia de los resync
# ═════════════════════════════════════════════════════════════════════════════
class TestIdempotenciaResync(unittest.TestCase):
    """El plan exige que resincronizar dos veces seguidas el mismo documento
    no duplique ni destruya nada."""

    def test_fetch_from_erp_reemplaza_lineas_no_las_acumula(self):
        """`_tr_fetch_from_erp` hace DELETE + INSERT de las líneas ZZ dentro de
        la misma transacción. Correr el resync N veces deja N=1 filas, no N."""
        fuente = _cuerpo_funcion("_tr_fetch_from_erp")
        sql = _norm(fuente)
        self.assertIn(
            "DELETE FROM transport_commitment_lines WHERE commitment_id", sql,
            "Desapareció el DELETE previo al INSERT de líneas: el resync "
            "podría estar duplicando filas.",
        )
        self.assertIn("INSERT INTO transport_commitment_lines", sql)

    def test_populate_item_lines_usa_INSERT_IGNORE(self):
        """`_tr_populate_item_lines` se llama desde varios sitios (alta de
        manifest_item, backfill perezoso al abrir el modal, backfill de boot).
        Todos pueden coincidir sobre el mismo item: INSERT IGNORE + UNIQUE es
        lo que evita duplicados."""
        sql = _norm(_cuerpo_funcion("_tr_populate_item_lines"))
        self.assertIn("INSERT IGNORE INTO transport_item_lines", sql)

    def test_limpiar_resync_solo_borra_documentos_sin_manifiesto(self):
        """El modo destructivo del resync (`limpiar_resync`) borra documentos
        de la ventana ANTES de volver a traerlos. La única protección real es
        que nunca toca un documento que ya esté en un manifiesto.

        Si este NOT EXISTS desaparece, un resync puede borrar trabajo
        operativo ya asignado a un chofer.
        """
        fuente = _cuerpo_funcion("_tr_sync_mes_actual_bg")
        sql = _norm(fuente)
        self.assertIn("DELETE", sql.upper())
        self.assertRegex(
            sql,
            r"NOT EXISTS\s*\(\s*SELECT 1 FROM transport_manifest_items",
            "El DELETE de limpiar_resync perdió la protección que impide "
            "borrar documentos ya asignados a un manifiesto.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 5 · Marcar "Entregado" — dónde se valida y dónde no
# ═════════════════════════════════════════════════════════════════════════════
class TestEstadoLogisticoVsEstadoErp(unittest.TestCase):
    """El Monitor mostraba "Despachado" en facturas emitidas el mismo día, sin
    courier y sin manifiesto — porque `estado` lo escribe el sync desde el saldo
    de la línea ZZ (el SERVICIO de flete), no desde la realidad logística."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_compromisos_json")

    def test_la_grilla_expone_un_estado_logistico_derivado_de_la_realidad(self):
        self.assertIn('"estado_logistico"', self.fuente,
                      "Desapareció estado_logistico: el Monitor vuelve a mostrar "
                      "el estado contable del ERP como si fuera logístico.")
        self.assertIn("Por despachar", self.fuente,
                      "Desapareció el estado 'Por despachar' (= no está en "
                      "ningún manifiesto, nadie lo tomó todavía).")

    def test_el_estado_del_erp_no_se_pierde_solo_se_renombra(self):
        """No se borra información: el estado del ERP sigue viajando en
        `estado` (lo usan el Kanban y los filtros) y además en `estado_erp`
        con nombre honesto, para mostrarlo como dato secundario."""
        self.assertIn('"estado_erp"', self.fuente)
        self.assertIn('"estado":', self.fuente)

    def test_el_estado_logistico_sale_del_item_de_manifiesto_mas_reciente(self):
        """Mismo criterio que courier/manifiesto_id: si no, la grilla mostraría
        el courier de un despacho y el estado de otro."""
        self.assertIn("estado_entrega_item", self.fuente)
        self.assertRegex(
            _norm(self.fuente),
            r"SELECT tmi\.estado_entrega FROM transport_manifest_items tmi.*ORDER BY tmi\.id DESC LIMIT 1",
        )


class TestAlertaEnviosEstancados(unittest.TestCase):
    """Válvula de escape del fix de multi-bulto: si a un envío se le pierde un
    bulto, ya no se cierra solo. Esta alerta evita que quede invisible."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_alertas_json")

    def test_solo_alerta_de_despachos_todavia_abiertos(self):
        """Si alguien ya lo cerró a mano, dejó de ser un problema: no tiene
        sentido seguir avisando."""
        self.assertIn("estado_entrega NOT IN", self.fuente)
        self.assertIn("parcial_desde IS NOT NULL", self.fuente)

    def test_respeta_un_umbral_de_dias(self):
        """Un envío que quedó parcial hace 20 minutos no es un problema — el
        otro bulto puede estar por llegar. La alerta es para lo que se estancó."""
        self.assertIn("DATEDIFF(NOW(), mi.parcial_desde) >= %s", self.fuente)

    def test_las_fechas_salen_en_hora_chile(self):
        """REGLA #6: nunca ISO crudo ni UTC en la UI."""
        self.assertIn("chile_fmt_filter", self.fuente)

    def test_devuelve_una_lista_extensible_de_alertas(self):
        """Pensado para que las alertas de conciliación del plan entren acá
        mismo, con la misma forma, en vez de inventar otro endpoint."""
        self.assertIn('"alertas"', self.fuente)
        self.assertIn('"codigo"', self.fuente)
        self.assertIn('"severidad"', self.fuente)

    def test_tiene_techo_de_resultados(self):
        self.assertRegex(_norm(self.fuente), r"LIMIT \d+")


class TestMarcarEntregado(unittest.TestCase):

    def test_GAP_kanban_permite_Entregado_con_saldo_pendiente(self):
        """[GAP] Arrastrar una tarjeta a la columna 'Entregados' del Kanban
        llama a PUT /transporte/api/compromisos/<id> con {estado:'Entregado'}.
        `tr_update_compromiso` valida que el estado esté en la lista de
        estados válidos, pero **no mira `tiene_saldo` ni `productos_json`**:
        se puede marcar entregada una factura con productos pendientes.

        El plan pide un test que impida exactamente esto. Por ahora lo
        DOCUMENTA (Fase 0); bloquearlo es cambio de comportamiento y va en la
        fase que corresponda, con el visto bueno de Daniel.

        CUANDO SE ARREGLE: este test debe invertirse — pasar a exigir que la
        validación de saldo exista.
        """
        fuente = _cuerpo_funcion("tr_update_compromiso")
        self.assertIn('"estado"', fuente, "Cambió el manejo de `estado` en el endpoint.")

        # Hoy NO hay ninguna mención a saldo/productos en la ruta que fija estado.
        for señal in ("tiene_saldo", "productos_json", "saldo_zz"):
            self.assertNotIn(
                señal, fuente,
                f"tr_update_compromiso ahora menciona '{señal}': puede que se "
                "haya agregado la validación de saldo. Actualizar este test.",
            )

    def test_funcion_central_bloquea_que_un_poller_saque_de_estado_terminal(self):
        """Comportamiento CORRECTO a preservar: `_tr_apply_carrier_status` no
        deja que una fuente automática (fedex/simpliroute) saque un despacho de
        un estado terminal. Un poller atrasado no puede "desentregar" un
        pedido; solo una persona puede."""
        fuente = _cuerpo_funcion("_tr_apply_carrier_status")
        self.assertIn("ESTADOS_ENTREGA_TERMINALES", fuente)
        self.assertIn("FUENTES_AUTOMATICAS", fuente)

    def test_GAP_hay_UPDATEs_de_estado_entrega_fuera_de_la_funcion_central(self):
        """[GAP] Varios caminos (endpoint manual de estado, POD manual, app del
        chofer) escriben `estado_entrega` con un UPDATE directo en vez de pasar
        por `_tr_apply_carrier_status`. Se saltan la guarda de continuidad y el
        chequeo de "cambió realmente".

        Es el pendiente ya registrado como SEV-7. Este test fija cuántos son
        hoy, para que no aparezcan MÁS sin que nadie se entere.

        CUANDO SE ARREGLE: bajar el número esperado, o pasar a exigir 0.
        """
        fuente = _fuente_app()
        # UPDATEs literales sobre estado_entrega en el fuente completo.
        ocurrencias = re.findall(r"SET\s+estado_entrega\s*=", fuente)
        self.assertGreaterEqual(
            len(ocurrencias), 2,
            "Ya no hay UPDATEs directos de estado_entrega — si se centralizaron "
            "todos, actualizar este test.",
        )
        self.assertLessEqual(
            len(ocurrencias), 8,
            f"Aparecieron más UPDATEs directos de estado_entrega ({len(ocurrencias)}) "
            "de los que había cuando se escribió este test (SEV-7). Todo estado "
            "nuevo debería pasar por _tr_apply_carrier_status.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 6 · Entrega parcial de SimpliRoute
# ═════════════════════════════════════════════════════════════════════════════
class TestSimpliRouteParcial(unittest.TestCase):
    """simpliroute_client es un módulo puro: se importa directo, sin AST."""

    @classmethod
    def setUpClass(cls):
        import simpliroute_client
        cls.sr = simpliroute_client

    def test_visita_completada_marca_Entregado(self):
        estado, _ = self.sr.estado_ilus_from_visit({"status": "completed"})
        self.assertEqual(estado, "Entregado")

    def test_visita_fallida_marca_Entrega_fallida(self):
        estado, comentario = self.sr.estado_ilus_from_visit(
            {"status": "failed", "checkout_comment": "nadie en el domicilio"}
        )
        self.assertEqual(estado, "Entrega fallida")
        self.assertIn("nadie en el domicilio", comentario)

    def test_visita_cancelada_no_cambia_el_estado(self):
        """Cancelar una visita en el sistema del transportista NO define el
        estado comercial en ILUS: es decisión operativa, no automática."""
        estado, _ = self.sr.estado_ilus_from_visit({"status": "canceled"})
        self.assertIsNone(estado)

    def test_pendiente_con_on_its_way_marca_En_ruta(self):
        estado, _ = self.sr.estado_ilus_from_visit(
            {"status": "pending", "on_its_way": True}
        )
        self.assertEqual(estado, "En ruta")

    def test_pendiente_sin_movimiento_no_cambia_nada(self):
        estado, _ = self.sr.estado_ilus_from_visit({"status": "pending"})
        self.assertIsNone(estado)

    def test_datos_basura_no_revientan(self):
        for basura in (None, "", 42, [], {"status": None}, {"status": "vaya-uno-a-saber"}):
            estado, comentario = self.sr.estado_ilus_from_visit(basura)
            self.assertIsNone(estado)
            self.assertIsInstance(comentario, str)

    def test_GAP_entrega_PARCIAL_se_marca_como_Entregado_completo(self):
        """[GAP] SimpliRoute distingue 'partial' (entregó parte de la carga)
        de 'completed'. ILUS colapsa las dos a 'Entregado'. Lo único que
        preserva la diferencia es el texto del comentario.

        Consecuencias hoy:
          · el saldo de productos NO se actualiza con lo realmente entregado;
          · el despacho queda en estado terminal, así que el poller ya no puede
            moverlo (ver guarda de _tr_apply_carrier_status);
          · un 'Despachado parcial' del ERP no tiene equivalente acá.

        Es justo el caso de "entrega parcial" que el plan quiere modelar de
        verdad. CUANDO SE ARREGLE: este test debe exigir un estado parcial
        propio y la baja de saldo correspondiente.
        """
        estado, comentario = self.sr.estado_ilus_from_visit({"status": "partial"})
        self.assertEqual(
            estado, "Entregado",
            "Cambió el mapeo de 'partial' — si ahora hay estado parcial propio, "
            "actualizar este test de caracterización.",
        )
        self.assertIn(
            "PARCIAL", comentario.upper(),
            "El comentario es hoy la ÚNICA huella de que fue parcial: no puede "
            "perderse mientras no exista un estado propio.",
        )

    def test_GAP_partial_y_completed_son_indistinguibles_por_estado(self):
        """[GAP, complemento del anterior] Explicitado como igualdad: quien lea
        solo `estado_entrega` no puede saber si la entrega fue completa."""
        parcial, _ = self.sr.estado_ilus_from_visit({"status": "partial"})
        completa, _ = self.sr.estado_ilus_from_visit({"status": "completed"})
        self.assertEqual(parcial, completa)


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 7 · Multi-bulto FedEx
# ═════════════════════════════════════════════════════════════════════════════
class TestFedexMultiBulto(unittest.TestCase):

    def test_el_poller_consulta_TODAS_las_piezas_no_solo_la_primera(self):
        """REGRESIÓN (arreglado 2026-07-31).

        Antes: `_fedex_poll_batch` consultaba solo la columna
        `tracking_number`, que en multi-bulto guarda únicamente el TN del
        PRIMER bulto. El despacho se marcaba Entregado en cuanto llegaba ese
        bulto, con los otros N-1 todavía en tránsito — y como Entregado es
        terminal, ningún poll posterior podía corregirlo.

        Ahora: lee `piece_trackings_json` y agrega el estado de todas las
        piezas. El comportamiento fino (cuándo marca Entregado, qué pasa con
        un bulto fallido o devuelto, el chunking de 30) se prueba de verdad,
        ejecutando la función con stubs, en tests/test_fedex_multibulto_poll.py.
        Este de acá solo evita que alguien vuelva a la query de una sola pieza.
        """
        poller = _cuerpo_funcion("_fedex_poll_batch")
        self.assertIn(
            "piece_trackings_json", poller,
            "El poller volvió a mirar solo `tracking_number`: puede marcar "
            "Entregado un despacho al que todavía le faltan bultos.",
        )

    def test_el_conteo_de_bultos_se_persiste_para_poder_alertar(self):
        """Antes el conteo "2 de 3" solo vivía dentro del JSON de un evento —
        imposible de filtrar con un WHERE. Ahora se guarda en columnas, que es
        lo que hace posible la alerta de envío estancado."""
        poller = _cuerpo_funcion("_fedex_poll_batch")
        for col in ("bultos_total", "bultos_entregados", "parcial_desde"):
            self.assertIn(col, poller,
                          f"El poller dejó de mantener {col}: la alerta de "
                          "envíos estancados se queda sin datos.")

    def test_parcial_desde_no_se_pisa_en_cada_poll(self):
        """CLAVE: `parcial_desde` marca cuándo quedó a medias. Si cada poll lo
        reescribiera con NOW(), el contador de días volvería a cero cada 15
        minutos y NINGÚN envío llegaría nunca al umbral de la alerta.
        Por eso se usa COALESCE: se escribe solo la primera vez."""
        poller = _norm(_cuerpo_funcion("_fedex_poll_batch"))
        self.assertIn("parcial_desde=COALESCE(parcial_desde, NOW())", poller,
                      "parcial_desde dejó de usar COALESCE: el contador de días "
                      "se reinicia en cada poll y la alerta nunca se dispara.")

    def test_parcial_desde_se_limpia_cuando_deja_de_estar_parcial(self):
        """Si llegan todos los bultos (o todavía ninguno), el envío ya no está
        parcial y tiene que salir de la alerta."""
        self.assertIn("parcial_desde=NULL", _norm(_cuerpo_funcion("_fedex_poll_batch")))

    def test_master_tracking_es_el_del_primer_bulto_en_el_fallback(self):
        """Deja fijado de dónde sale el 'master' cuando son N envíos sueltos:
        es el tracking del bulto 1, no un identificador propio de FedEx."""
        fuente = _cuerpo_funcion("_fedex_create_individual_pieces")
        self.assertRegex(
            _norm(fuente),
            r"master_tracking_number[\"']?\s*[:=]\s*all_pieces\[0\]",
            "Cambió el origen del master_tracking_number en el fallback "
            "multi-bulto.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 8 · Una misma factura en dos manifiestos
# ═════════════════════════════════════════════════════════════════════════════
class TestFacturaEnDosManifiestos(unittest.TestCase):

    def test_GAP_confirm_dup_permite_la_misma_factura_en_dos_manifiestos(self):
        """[GAP] `tr_asignar_a_manifiesto` avisa con 409 si la factura ya está
        en otro manifiesto, PERO el aviso se puede saltar con `confirm_dup`.
        Y la unicidad de la tabla es sobre el par (manifest_id, commitment_id),
        no sobre commitment_id: la base lo permite.

        Queda entonces la misma factura viva en dos manifiestos, cada uno con
        su propio `estado_entrega` evolucionando por separado — sin ningún
        mecanismo que los reconcilie.

        Nota: esto también es el único camino que hoy permitiría separar
        despacho e instalación de una misma factura, así que NO es obvio que
        haya que prohibirlo. Se documenta para decidirlo en Fase 1.
        """
        fuente = _cuerpo_funcion("tr_asignar_a_manifiesto")
        self.assertIn(
            "confirm_dup", fuente,
            "Desapareció el escape `confirm_dup` — si se prohibió el duplicado, "
            "actualizar este test.",
        )
        self.assertIn("INSERT IGNORE INTO transport_manifest_items", _norm(fuente))

    def test_unique_key_es_sobre_el_par_no_sobre_commitment_id(self):
        """Lo que hace posible el caso de arriba. Si algún día se endurece a
        UNIQUE(commitment_id), este test avisa que el modelo cambió."""
        self.assertRegex(
            _norm(_fuente_app()),
            r"UNIQUE KEY uq_item \(manifest_id, ?commitment_id\)",
            "Cambió la unicidad de transport_manifest_items.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 9 · Columna `ramo` en transport_manifest_items (Fase 1: modelo de dos
# ramos, primer paso -- solo modelo de datos, sin cambiar comportamiento)
# ═════════════════════════════════════════════════════════════════════════════
class TestRamoManifestItems(unittest.TestCase):
    """Primer paso hacia el modelo de dos ramos independientes (ver
    TestClasificacionZZ.test_GAP_envio_mas_instalacion_pierde_el_envio, arriba):
    una columna `ramo` en transport_manifest_items + backfill desde
    transport_commitments.clasificacion. Este paso NO cambia todavía qué ramo
    recibe un item nuevo al crearse (sigue quedando NULL) -- eso es trabajo de
    una fase posterior."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_ensure_transport_tracking_tables")
        cls.sql = _norm(cls.fuente)

    def test_agrega_la_columna_ramo_con_el_mismo_vocabulario_que_clasificacion(self):
        """`ramo` tiene que hablar el mismo vocabulario que
        transport_commitments.clasificacion (despacho/retiro/instalacion/
        mantencion/garantia), para que el backfill -- y cualquier alta futura
        por item -- no inventen strings nuevos."""
        self.assertIn(
            '"ramo"', self.fuente,
            "Desapareció la entrada 'ramo' del dict item_cols de "
            "_ensure_transport_tracking_tables.",
        )
        self.assertIn(
            "ENUM('despacho','retiro','instalacion','mantencion','garantia')",
            self.sql,
            "El ENUM de `ramo` dejó de coincidir con el de "
            "transport_commitments.clasificacion.",
        )

    def test_ramo_es_nullable(self):
        """NULL a propósito: los items existentes no tienen ramo hasta el
        backfill, y el alta de items nuevos todavía no lo setea (paso
        pendiente, ver comentario en el dict item_cols)."""
        self.assertRegex(
            self.sql,
            # [\s"]* en vez de \s+: el DDL es una concatenación de strings de
            # Python en el código fuente ("...) " "NULL...") — _cuerpo_funcion
            # devuelve el texto TAL CUAL está escrito, comillas de
            # concatenación incluidas, no el valor ya evaluado. En tiempo de
            # ejecución Python las concatena y el DDL real queda correcto.
            r"ENUM\('despacho','retiro','instalacion','mantencion','garantia'\)[\s\"]*NULL",
            "`ramo` dejó de ser NULLABLE.",
        )

    def test_agrega_la_columna_via_alter_idempotente_igual_que_el_resto_del_dict(self):
        """Mismo patrón que tracking_number/bultos_total/etc: ALTER TABLE ADD
        COLUMN solo si information_schema dice que no existe todavía."""
        self.assertIn(
            "ALTER TABLE transport_manifest_items ADD COLUMN {col} {ddl}",
            self.fuente,
            "El agregado de columnas de transport_manifest_items dejó de "
            "pasar por el loop idempotente compartido -- revisar si `ramo` "
            "sigue entrando por ahí.",
        )

    def test_backfill_solo_toca_filas_sin_ramo(self):
        """El UPDATE...JOIN de backfill tiene que ser idempotente: si ya
        corrió antes (o alguien fijó `ramo` a mano en una fase futura), no lo
        debe pisar."""
        self.assertIn(
            "WHERE mi.ramo IS NULL", self.sql,
            "El backfill de ramo perdió el filtro WHERE ramo IS NULL: podría "
            "pisar un ramo ya asignado.",
        )

    def test_backfill_copia_la_clasificacion_del_commitment_asociado(self):
        """Fuente del backfill: transport_commitments.clasificacion vía JOIN
        por commitment_id -- es el único dato de clasificación que existe hoy
        (uno por documento, no por item)."""
        self.assertRegex(
            self.sql,
            r"UPDATE transport_manifest_items mi"
            r"\s+JOIN transport_commitments c ON c\.id\s*=\s*mi\.commitment_id"
            r"\s+SET mi\.ramo\s*=\s*c\.clasificacion",
            "Cambió la forma del backfill de ramo (join o columna origen).",
        )

    def test_backfill_corre_despues_del_alter_que_agrega_la_columna(self):
        """Si el backfill corriera ANTES del ALTER TABLE, reventaría en una
        base de datos que recién agrega `ramo` (columna todavía no existe)."""
        idx_alter = self.fuente.index("ALTER TABLE transport_manifest_items ADD COLUMN {col} {ddl}")
        idx_backfill = self.fuente.index("backfill ramo aplicado")
        self.assertLess(
            idx_alter, idx_backfill,
            "El backfill de ramo quedó antes del ALTER TABLE que agrega la "
            "columna: en un boot limpio reventaría con 'Unknown column ramo'.",
        )

    def test_backfill_esta_envuelto_en_su_propio_try_except(self):
        """Mismo estilo que el resto de la función (ej. backfill tiene_saldo=1
        más abajo): un fallo del backfill no debe tumbar el resto del boot."""
        self.assertIn("except Exception as _e_ramo:", self.fuente)


if __name__ == "__main__":
    unittest.main(verbosity=2)

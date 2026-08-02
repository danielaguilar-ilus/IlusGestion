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

    def test_envio_mas_instalacion_sigue_colapsando_a_un_solo_valor_por_diseno(self):
        """RESUELTO en PR-B (2026-08-01) a nivel de SISTEMA -- pero
        `_clasif_from_skus` en sí NO se tocó a propósito (instrucción
        explícita: sigue siendo la fuente de
        transport_commitments.clasificacion, un valor legacy por documento
        que otros lugares del código todavía leen).

        Antes esto vivía acá como test_GAP_envio_mas_instalacion_pierde_el_envio:
        documentaba que la señal de "también hay que despachar" se perdía
        del todo, sin ningún mecanismo que la recuperara. Eso YA NO es
        cierto a nivel de sistema -- ver TestClasificacionZZMulti
        (_clasifs_from_skus_multi, que SÍ devuelve los dos ramos) y
        TestPlanRamoManifestItem / TestWriteSitesRamoMultiple más abajo (los
        4 write-sites que crean transport_manifest_items con ramo real,
        usando esa función multi, no esta). Este test se deja para fijar que
        _clasif_from_skus, puntualmente, sigue colapsando -- si algún día se
        la hace multi-valor, hay que revisar todo lo que lee `clasificacion`
        como single-value antes de tocarla.
        """
        self.assertEqual(self.clasif(["ZZENVIO", "ZZINSTALACION"]), "instalacion")

    def test_GAP_retiro_solo_gana_si_TODAS_las_lineas_son_retiro(self):
        """[GAP] 'retiro' usa all(), el resto usa any(). Un documento con
        ZZRETIRO + ZZENVIO NO es retiro: cae a 'despacho'. Documentado acá
        porque es una asimetría fácil de romper sin querer al refactorizar."""
        self.assertEqual(self.clasif(["ZZRETIRO", "ZZENVIO"]), "despacho")


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 1.b · _clasifs_from_skus_multi — Fase 1 PR-B, detecta TODOS los ramos
# ═════════════════════════════════════════════════════════════════════════════
class TestClasificacionZZMulti(unittest.TestCase):
    """La pieza que resuelve el GAP de arriba (test_GAP_envio_mas_instalacion_
    pierde_el_envio): a diferencia de _clasif_from_skus (single, con
    prioridad), esta devuelve TODOS los ramos presentes. En esta pieza (PR-B
    paso 1) todavía NADIE la usa para escribir -- eso es el paso siguiente
    (los 4 write-sites). Acá solo se prueba la función en sí."""

    @classmethod
    def setUpClass(cls):
        # _clasifs_from_skus_multi usa la constante de módulo _ORDEN_RAMOS --
        # se extrae su valor REAL del código fuente (no se hardcodea un
        # duplicado en el test, que podría desincronizarse si alguien cambia
        # el orden en app.py sin tocar el test).
        _orden_ramos = None
        for nodo in ast.walk(_arbol_app()):
            if isinstance(nodo, ast.Assign):
                for destino in nodo.targets:
                    if isinstance(destino, ast.Name) and destino.id == "_ORDEN_RAMOS":
                        _orden_ramos = ast.literal_eval(nodo.value)
        if _orden_ramos is None:
            raise AssertionError("No se encontró _ORDEN_RAMOS en app.py")
        cls.multi = staticmethod(
            _extraer_funcion("_clasifs_from_skus_multi", extras={"_ORDEN_RAMOS": _orden_ramos}))

    def test_solo_zzenvio_da_un_solo_ramo(self):
        self.assertEqual(self.multi(["ZZENVIO"]), ["despacho"])

    def test_envio_mas_instalacion_da_AMBOS_ramos(self):
        """El caso central: a diferencia de la versión single (que colapsa a
        'instalacion' solo), acá deben aparecer los DOS."""
        self.assertEqual(self.multi(["ZZENVIO", "ZZINSTALACION"]), ["despacho", "instalacion"])

    def test_orden_es_siempre_despacho_primero_no_depende_del_input(self):
        self.assertEqual(self.multi(["ZZINSTALACION", "ZZENVIO"]), ["despacho", "instalacion"])

    def test_sin_skus_cae_a_despacho_por_defecto(self):
        self.assertEqual(self.multi([]), ["despacho"])
        self.assertEqual(self.multi([None, "", "  "]), ["despacho"])

    def test_no_duplica_ramos_si_hay_varias_lineas_del_mismo_tipo(self):
        self.assertEqual(self.multi(["ZZINSTALACION", "ZZINSTALACION"]), ["instalacion"])

    def test_es_case_insensitive(self):
        self.assertEqual(self.multi(["zzenvio", "zzinstalacion"]), ["despacho", "instalacion"])

    def test_retiro_mas_envio_da_AMBOS_a_diferencia_de_la_version_single(self):
        """Asimetría intencional respecto a _clasif_from_skus (que usa all()
        para retiro y por eso ZZRETIRO+ZZENVIO colapsa a solo 'despacho'):
        en la versión multi, cualquier ramo con una línea ZZ real presente se
        refleja -- mismo criterio ya aplicado a despacho+instalación."""
        self.assertEqual(self.multi(["ZZRETIRO", "ZZENVIO"]), ["despacho", "retiro"])

    def test_tres_ramos_a_la_vez(self):
        self.assertEqual(
            self.multi(["ZZENVIO", "ZZINSTALACION", "ZZSERVTEC"]),
            ["despacho", "instalacion", "mantencion"],
        )

    def test_mantencion_agrupa_los_tres_skus_tecnicos(self):
        for sku in ("ZZSERVTEC", "ZZINGREPUESTO", "ZZINGARREQUIP"):
            with self.subTest(sku=sku):
                self.assertEqual(self.multi([sku]), ["mantencion"])


class TestRamosDeCommitment(unittest.TestCase):
    """_tr_ramos_de_commitment lee zz_skus YA PERSISTIDO (sin volver a tocar
    el ERP) y lo pasa por _clasifs_from_skus_multi. Se prueba solo la
    ESTRUCTURA (no se puede ejecutar sin MySQL real) -- que lea la columna
    correcta y delegue en la función multi, no reimplemente el mapeo."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _norm(_cuerpo_funcion("_tr_ramos_de_commitment"))

    def test_lee_zz_skus_de_transport_commitments(self):
        self.assertIn("SELECT zz_skus FROM transport_commitments WHERE id=%s", self.fuente)

    def test_delega_en_la_funcion_multi_no_reimplementa_el_mapeo(self):
        self.assertIn("_clasifs_from_skus_multi(skus)", self.fuente,
                      "Dejó de delegar en _clasifs_from_skus_multi -- si "
                      "reimplementa el mapeo acá, las dos copias del "
                      "criterio SKU→ramo pueden desincronizarse.")

    def test_parsea_la_lista_compacta_separada_por_comas(self):
        self.assertIn('.split(",")', self.fuente)


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 1.b · _tr_ramo_where (Monitor por ramo, 2026-08-01)
# ═════════════════════════════════════════════════════════════════════════════
class TestTrRamoWhere(unittest.TestCase):
    """_tr_ramo_where arma el fragmento SQL parametrizado que filtra el
    Monitor por ramo SIN depender solo de `clasificacion` (columna colapsada
    a 1 valor) -- agrega un OR contra zz_skus para que un documento con
    zz_skus='ZZENVIO,ZZINSTALACION' aparezca tanto en Despacho como en
    Instalación. Se prueba EJECUTANDO la función real (es pura: str in,
    (str,list) out) contra un fake mysql_fetchall/fetchone -- no hace falta
    MySQL real porque no toca la BD, solo arma el fragmento SQL."""

    @classmethod
    def setUpClass(cls):
        _ramo_skus = None
        for nodo in ast.walk(_arbol_app()):
            if isinstance(nodo, ast.Assign):
                for destino in nodo.targets:
                    if isinstance(destino, ast.Name) and destino.id == "_RAMO_SKUS":
                        _ramo_skus = ast.literal_eval(nodo.value)
        if _ramo_skus is None:
            raise AssertionError("No se encontró _RAMO_SKUS en app.py")
        cls.fn = staticmethod(_extraer_funcion("_tr_ramo_where", extras={"_RAMO_SKUS": _ramo_skus}))

    def test_ramo_vacio_no_filtra(self):
        self.assertEqual(self.fn(""), (None, []))
        self.assertEqual(self.fn(None), (None, []))

    def test_despacho_agrega_or_contra_zzenvio(self):
        sql, params = self.fn("despacho")
        self.assertIn("clasificacion=%s", sql)
        self.assertIn("zz_skus LIKE %s", sql)
        self.assertIn(" OR ", sql)
        self.assertEqual(params, ["despacho", "%ZZENVIO%"])

    def test_instalacion_agrega_or_contra_zzinstalacion(self):
        sql, params = self.fn("instalacion")
        self.assertEqual(params, ["instalacion", "%ZZINSTALACION%"])

    def test_retiro_agrega_or_contra_zzretiro(self):
        sql, params = self.fn("retiro")
        self.assertEqual(params, ["retiro", "%ZZRETIRO%"])

    def test_mantencion_agrega_or_contra_los_tres_skus(self):
        sql, params = self.fn("mantencion")
        self.assertEqual(params, ["mantencion", "%ZZSERVTEC%", "%ZZINGREPUESTO%", "%ZZINGARREQUIP%"])
        # clasificacion=%s OR (3 LIKE unidos por 2 " OR ") = 3 " OR " en total.
        self.assertEqual(sql.count(" OR "), 3)

    def test_garantia_sin_sku_asociado_usa_solo_columna_directa(self):
        """garantia se asigna manual, no se deriva de ningún SKU -- no debe
        agregar ningún OR contra zz_skus (evita falsos positivos)."""
        sql, params = self.fn("garantia")
        self.assertEqual((sql, params), ("clasificacion=%s", ["garantia"]))

    def test_mayusculas_y_espacios_no_rompen_el_match(self):
        sql, params = self.fn("  INSTALACION  ")
        self.assertEqual(params, ["instalacion", "%ZZINSTALACION%"])


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

    def test_sin_saldo_es_Despachado_no_Entregado(self):
        """Regresión histórica (FIX 2026-07-25): "sin saldo" significa
        'facturado/cubierto en el ERP', NO 'entregado físicamente'. Alguna vez
        este código escribió 'Entregado' y mezcló plata con logística.

        Este test existe para que ese error no vuelva.
        """
        self.assertRegex(
            _norm(self.fuente),
            r"if not _saldo_fisico: estado_auto = \"Despachado\"",
        )
        # Y que el bulk sync NO escriba 'Entregado' como estado automático.
        estados_auto = re.findall(r'estado_auto\s*=\s*"([^"]+)"', self.fuente)
        self.assertNotIn(
            "Entregado", estados_auto,
            "El bulk sync volvió a derivar 'Entregado' desde el saldo del ERP. "
            "Eso confunde saldo financiero con entrega física (bug de 2026-07-25).",
        )

    def test_el_saldo_que_decide_sale_del_producto_fisico_no_del_ZZ(self):
        """FIX 2026-08-01 (Daniel, caso real FCV 0000011152 + informe oficial
        del ERP): el flag tiene_saldo -- que decide la pestaña Pendientes vs
        Entregados -- debe salir del saldo del PRODUCTO FÍSICO, no de
        saldo_zz (líneas de servicio, que el ERP cierra contablemente apenas
        se factura, con la mercadería todavía en bodega).

        "Esto es una página logística: los servicios no mueven las facturas,
        los productos sí" -- Daniel.
        """
        cuerpo = _norm(self.fuente)
        self.assertIn("_saldo_fisico = int(row.get(\"tiene_saldo_fisico\") or 0)", cuerpo,
                      "El bulk sync dejó de leer tiene_saldo_fisico del SQL.")
        self.assertNotRegex(
            cuerpo, r"if saldo_zz <= 0: estado_auto",
            "Volvió a decidir el estado con saldo_zz (líneas de servicio). "
            "Ese es exactamente el bug de FCV 0000011152.",
        )
        # El SQL tiene que calcular el flag sobre líneas NO-ZZ.
        self.assertIn("AS tiene_saldo_fisico", self.sql)
        self.assertRegex(
            _norm(self.sql),
            r"FROM MAEDDO dsf.{0,400}?<> 'ZZ'",
            "El EXISTS de tiene_saldo_fisico dejó de excluir las líneas ZZ.",
        )

    def test_documento_sin_lineas_fisicas_se_conserva_pendiente(self):
        """RED DE SEGURIDAD (Daniel 2026-08-01: "puede dejar a un cliente sin
        despacho... riesgo reputacional"). Un documento de PURO servicio (sin
        ninguna línea de producto) no tiene saldo físico que medir -- si se
        dejara caer a 0 desaparecería de Pendientes aunque alguien todavía
        tenga que ir a instalar. Debe conservarse visible.

        Ante la duda se conserva: un falso pendiente cuesta una revisión, un
        falso entregado cuesta un cliente sin su pedido.
        """
        self.assertRegex(
            _norm(self.sql),
            r"OR NOT EXISTS \(.{0,300}?FROM MAEDDO dnf.{0,200}?<> 'ZZ'",
            "El bulk sync perdió la red de seguridad para documentos sin "
            "líneas de producto físico (puro servicio).",
        )
        # El sync individual tiene que tener la MISMA red de seguridad.
        # (regex, no assertIn: entre el `if` y la asignación hay un comentario
        # explicativo que _norm conserva.)
        cuerpo_ind = _norm(_cuerpo_funcion("_tr_fetch_from_erp"))
        self.assertRegex(
            cuerpo_ind, r"if not _fis_lines:.{0,300}?tiene_saldo = 1",
            "El sync individual perdió el fallback a pendiente "
            "cuando el documento no trae líneas de producto.",
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

    def test_catchup_de_guia_en_documentos_terminales_sin_guia(self):
        """REGRESIÓN real (2026-08-01, caso BLV 22727): un documento que llega
        a estado terminal ANTES de que este cron alcance a sincronizar su guía
        quedaba excluido PARA SIEMPRE por el filtro de "no terminal" — la guía
        nunca se capturaba aunque el ERP sí la tuviera.

        Daniel: "se deja de cumplir cuando se cumplen las DOS condiciones...
        si uno de esos dos procesos mata al otro, queda siempre incompleto".
        El corte real: dejar de tocar un documento exige terminal Y con guía,
        no solo terminal.
        """
        self.assertIn(
            "NOT EXISTS (SELECT 1 FROM transport_guias g WHERE g.commitment_id = c.id)",
            _norm(self.fuente),
            "Desapareció el catch-up de guía para documentos terminales — "
            "volvería el bug de BLV 22727 (guía real en el ERP, nunca "
            "capturada porque el documento ya estaba Entregado).",
        )

    def test_el_catchup_de_guia_esta_acotado_en_el_tiempo(self):
        """Sin un límite temporal, cada corrida barrería TODO el historial de
        documentos terminales sin guía para siempre (muchos de ellos
        legítimamente sin guía — boletas antiguas, documentos previos a esta
        feature) — un costo creciente sin techo contra el ERP."""
        self.assertIn("DATE_SUB(NOW(), INTERVAL 60 DAY)", _norm(self.fuente))

    def test_el_limite_de_tiempo_no_excluye_fecha_emision_nula(self):
        """REGRESIÓN real (2026-08-01, caso BLV 22727/commitment 141767): en
        SQL, `NULL >= DATE_SUB(...)` es NULL (ni true ni false) — sin el OR
        que trata NULL como "sí, revisar", un commitment con fecha_emision
        vacía (síntoma del mismo bug de commitments duplicados que afectó a
        BLV 22719) quedaba excluido del catch-up PARA SIEMPRE, exactamente el
        caso que este catch-up existe para arreglar."""
        self.assertIn(
            "c.fecha_emision IS NULL OR c.fecha_emision >= DATE_SUB", _norm(self.fuente),
            "El filtro de 60 días volvió a excluir fecha_emision NULL — un "
            "commitment con fecha vacía nunca entraría al catch-up.",
        )

    def test_el_catchup_no_llama_al_refresco_completo_del_commitment(self):
        """Un documento terminal no necesita refrescar saldo/cliente/dirección
        (ya no importan en un pedido cerrado) — el catch-up debe llamar
        directo a _tr_fetch_guias_from_erp con el id ya conocido, sin pasar
        por _tr_fetch_from_erp (que hace mucho más trabajo del necesario)."""
        self.assertIn(
            '_tr_fetch_guias_from_erp(d["id"], d["tido"], str(d["nudo"]))', self.fuente,
            "Desapareció la llamada directa de catch-up (con el id ya "
            "conocido de la query) — revisar si el catch-up sigue existiendo.",
        )
        # El fragmento entre el `else:` del catch-up y el `except` que cierra
        # el for no debe llamar a _tr_fetch_from_erp — esa es la rama pesada
        # (saldo/cliente/dirección), innecesaria para un documento ya cerrado.
        idx_else = self.fuente.index("# Catch-up: documento YA terminal")
        idx_except = self.fuente.index("except Exception as e:", idx_else)
        fragmento_catchup = self.fuente[idx_else:idx_except]
        self.assertNotIn(
            "_tr_fetch_from_erp(", fragmento_catchup,
            "El catch-up de guía dejó de ser la rama liviana — ahora también "
            "llama a _tr_fetch_from_erp, el refresco completo innecesario "
            "para un documento ya terminal.",
        )

    def test_sigue_acotado_a_despachos_no_terminales_para_el_saldo(self):
        """El alcance de SALDO (distinto del catch-up de guía) sigue exigiendo
        no-terminal + tiene_saldo, tal como se pidió el 2026-07-31."""
        self.assertIn("estado_entrega NOT IN", self.fuente)
        for terminal in ("Entregado", "Entrega fallida", "Devolución"):
            self.assertIn(terminal, self.fuente)


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

    def test_GAP_arreglado_ya_no_hay_UPDATEs_de_estado_entrega_fuera_de_la_funcion_central(self):
        """[GAP RESUELTO 2026-08-01] Antes, 5 caminos (endpoint manual de
        estado, POD manual de escritorio, escaneo QR / "Navegar" / POD del
        chofer) escribían `estado_entrega` con un UPDATE directo en vez de
        pasar por `_tr_apply_carrier_status` — se saltaban la guarda de
        continuidad y el chequeo de "cambió realmente" (registrado como
        SEV-7). Los 5 se migraron a `_tr_apply_carrier_status()`:
          - tr_estado_entrega
          - tr_registrar_entrega
          - chofer_captura_scan
          - chofer_parada_iniciar
          - chofer_entrega_submit
        (ver TestMigracionCincoSitiosAEstadoCentral más abajo, que verifica
        cada uno). Ahora el ÚNICO `SET estado_entrega=` literal que debe
        quedar en todo app.py es el de dentro de _tr_apply_carrier_status.

        Si este test vuelve a fallar porque el número subió, alguien agregó
        un UPDATE manual nuevo en vez de usar _tr_apply_carrier_status —
        revertir y usar el choke-point.
        """
        fuente = _fuente_app()
        # UPDATEs literales sobre estado_entrega en el fuente completo.
        ocurrencias = re.findall(r"SET\s+estado_entrega\s*=", fuente)
        self.assertEqual(
            len(ocurrencias), 1,
            f"Se esperaba UN solo UPDATE literal de estado_entrega (dentro de "
            f"_tr_apply_carrier_status), se encontraron {len(ocurrencias)}. "
            "Todo estado nuevo debería pasar por _tr_apply_carrier_status.",
        )
        # Y ese único UPDATE vive efectivamente dentro del choke-point.
        central = _cuerpo_funcion("_tr_apply_carrier_status")
        self.assertEqual(
            len(re.findall(r"SET\s+estado_entrega\s*=", central)), 1,
            "El UPDATE de estado_entrega que sobrevive ya no está dentro de "
            "_tr_apply_carrier_status — investigar dónde quedó.",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 5.b · Migración de los 5 sitios manuales a _tr_apply_carrier_status
# (2026-08-01) — resuelve el GAP de BLOQUE 5.
# ═════════════════════════════════════════════════════════════════════════════
class TestMigracionCincoSitiosAEstadoCentral(unittest.TestCase):
    """Los 5 sitios que actualizaban `estado_entrega` con su propio patrón
    manual (UPDATE + _tr_log + _tr_event) ahora delegan en
    `_tr_apply_carrier_status()`, el choke-point único descrito en su propio
    docstring. Mismo resultado observable (mismo estado final, mismo
    fuente/comentario/notify_cliente, mismas guardas de negocio que cada sitio
    ya tenía) — solo cambia que la lógica de estado_entrega+log+event dejó de
    estar duplicada 5 veces."""

    SITIOS = [
        "tr_estado_entrega",
        "tr_registrar_entrega",
        "chofer_captura_scan",
        "chofer_parada_iniciar",
        "chofer_entrega_submit",
    ]

    @classmethod
    def setUpClass(cls):
        cls.fuentes = {n: _cuerpo_funcion(n) for n in cls.SITIOS}
        cls.norm = {n: _norm(f) for n, f in cls.fuentes.items()}

    def test_ninguno_arma_ya_su_propio_UPDATE_de_estado_entrega(self):
        for nombre, fuente in self.fuentes.items():
            self.assertNotIn(
                "SET estado_entrega", _norm(fuente),
                f"{nombre} todavía arma su propio UPDATE de estado_entrega; "
                "debería delegar en _tr_apply_carrier_status.",
            )

    def test_los_5_llaman_a_la_funcion_central(self):
        for nombre, fuente in self.fuentes.items():
            self.assertIn(
                "_tr_apply_carrier_status(", fuente,
                f"{nombre} no llama a _tr_apply_carrier_status.",
            )

    def test_tr_estado_entrega_usa_fuente_manual_y_sigue_validando_el_manifiesto(self):
        n = self.norm["tr_estado_entrega"]
        self.assertIn("fuente='manual'", n)
        # Guarda agregada en la migración: el viejo UPDATE llevaba
        # "AND manifest_id=%s"; _tr_apply_carrier_status solo filtra por id,
        # así que el chequeo de scope se hace ahora ANTES de llamarlo.
        self.assertIn("AND manifest_id=%s", n)

    def test_tr_registrar_entrega_usa_fuente_manual_estado_entregado_y_gps(self):
        n = self.norm["tr_registrar_entrega"]
        self.assertIn("fuente='manual'", n)
        self.assertIn("'Entregado'", n)
        self.assertIn("lat=lat", n)
        self.assertIn("lng=lng", n)

    def test_chofer_captura_scan_usa_fuente_chofer_y_mantiene_guarda_de_no_retroceso(self):
        n = self.norm["chofer_captura_scan"]
        self.assertIn("fuente='chofer'", n)
        self.assertIn("'Entregado a transporte'", n)
        # La guarda pre-existente (no pisar En ruta/Entregado) se mantiene
        # delante de la llamada centralizada — _tr_apply_carrier_status NO
        # la reemplaza porque 'chofer' está exenta de su propia guarda
        # anti-retroceso (esa solo aplica a FUENTES_AUTOMATICAS).
        self.assertIn("not in ('En ruta', 'Entregado')", n)

    def test_chofer_parada_iniciar_usa_fuente_chofer_estado_en_ruta_y_early_return(self):
        n = self.norm["chofer_parada_iniciar"]
        self.assertIn("fuente='chofer'", n)
        self.assertIn("'En ruta'", n)
        # El early-return idempotente pre-existente se mantiene: si ya está
        # En ruta/Entregado/Devolución, no debe ni llamar a la función central.
        self.assertIn('"ya_estaba": True', n)

    def test_chofer_entrega_submit_usa_fuente_chofer_estado_entregado_y_gps(self):
        n = self.norm["chofer_entrega_submit"]
        self.assertIn("fuente='chofer'", n)
        self.assertIn("'Entregado'", n)
        self.assertIn("lat=lat", n)
        self.assertIn("lng=lng", n)

    def test_ninguno_desactiva_notify_cliente(self):
        """Los 5 sitios notificaban al cliente por default (ninguna llamada
        vieja a _tr_event pasaba notify_cliente=False). La migración debe
        preservar ese default (True) en los 5 — si alguno empieza a pasar
        notify_cliente=False, es un cambio de comportamiento de negocio no
        pedido."""
        for nombre, fuente in self.fuentes.items():
            self.assertNotIn(
                "notify_cliente=False", _norm(fuente),
                f"{nombre} pasa notify_cliente=False — antes de la migración "
                "notificaba al cliente por default (True) vía _tr_event().",
            )

    def test_funcion_central_soporta_lat_lng_para_no_perder_el_gps_del_chofer(self):
        """tr_registrar_entrega y chofer_entrega_submit le pasaban lat/lng
        directo a _tr_event() (GPS de dónde se firmó la entrega).
        _tr_apply_carrier_status no tenía esos parámetros — se extendió con
        lat=None, lng=None (default compatible con los otros 13 callers
        preexistentes, que no los usan) para poder migrar estos 2 sitios sin
        perder el dato."""
        central = _norm(_cuerpo_funcion("_tr_apply_carrier_status"))
        self.assertIn("lat=None, lng=None", central)
        self.assertIn("lat=lat, lng=lng", central,
                       "_tr_apply_carrier_status ya no reenvía lat/lng a _tr_event().")


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


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 7 · Guías de despacho reales (transport_guias) — 2026-08-01
# ═════════════════════════════════════════════════════════════════════════════
# Confirmado contra el ERP real (BLV 22719 → GDV 0000032281): la guía no es
# un campo de la factura, es un documento propio (TIDO='GDV'); cada línea de
# MAEDDO trae TIDOPA/NUDOPA/ENDOPA/NULIDOPA apuntando al documento de origen.
# Ver /api/erp/peek-guias para el diagnóstico que validó el modelo.

class TestGuiasTablaIdempotente(unittest.TestCase):
    """`_ensure_transport_guias_table` tiene que seguir EXACTAMENTE el mismo
    patrón que el resto de las tablas `_ensure_*` de transporte: CREATE TABLE
    IF NOT EXISTS, corre siempre en boot aunque ILUS_SKIP_MIGRATIONS=1."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_ensure_transport_guias_table")
        cls.sql = _norm(cls.fuente)

    def test_create_table_if_not_exists(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS transport_guias", self.sql)

    def test_tiene_las_columnas_pedidas(self):
        for col in ("commitment_id", "guia_tido", "guia_nudo", "guia_linea",
                    "sku", "producto", "cantidad", "fecha_guia", "estado_guia",
                    "codigo_transportista", "sincronizado_at"):
            self.assertIn(col, self.sql, f"Falta la columna {col!r} en transport_guias.")

    def test_tiene_indice_por_commitment_id(self):
        self.assertIn("INDEX idx_tg_commitment (commitment_id)", self.sql)

    def test_tiene_foreign_key_a_commitments(self):
        self.assertIn(
            "FOREIGN KEY (commitment_id) REFERENCES transport_commitments(id) ON DELETE CASCADE",
            self.sql,
        )

    def test_usa_mysql_execute_no_conexion_manual(self):
        """Mismo estilo simple que _ensure_transport_tracking_tables (un solo
        CREATE, sin necesitar cursor/commit manual)."""
        self.assertIn("mysql_execute(", self.fuente)

    def test_se_registra_en_el_boot_siempre_incluso_con_skip_migrations(self):
        """El CREATE TABLE por sí solo no sirve si nadie lo llama en boot --
        mismo gotcha documentado para el resto de las tablas nuevas de
        transporte (init_transporte_tables NO corre en prod).

        `_ensure_transport_guias_table()` aparece como substring literal de
        su propia línea `def _ensure_transport_guias_table():` (todo lo que
        sigue a "def " coincide) -- por eso NO basta con buscar el nombre una
        vez: tiene que aparecer una SEGUNDA vez, la llamada real de boot."""
        ocurrencias = _fuente_app().count("_ensure_transport_guias_table()")
        self.assertGreaterEqual(
            ocurrencias, 2,
            "_ensure_transport_guias_table() solo aparece 1 vez en app.py (su "
            "propia definición) -- falta la llamada de boot que la registra "
            "SIEMPRE, incluso con ILUS_SKIP_MIGRATIONS=1. La tabla nunca se "
            "crearía en producción.",
        )
        self.assertIn(
            "with app.app_context():\n        _ensure_transport_guias_table()",
            _fuente_app(),
            "La llamada de boot a _ensure_transport_guias_table() no sigue el "
            "mismo patrón app_context() que el resto de las tablas nuevas.",
        )


class TestGuiasFetchDesdeErp(unittest.TestCase):
    """`_tr_fetch_guias_from_erp` es el único punto que escribe en
    transport_guias. Tiene que usar el canal blindado read-only, resincronizar
    sin duplicar, y nunca poder tumbar al caller (cron horario / modal)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_tr_fetch_guias_from_erp")
        cls.sql = _norm(cls.fuente)
        cls.sql_sin_espacios = cls.sql.replace(" ", "")

    def test_usa_el_canal_blindado_random_sql(self):
        """Nunca pymssql/otro cliente directo -- SIEMPRE _random_sql_query /
        _random_sql_one (whitelist SELECT, parametrizado, autocommit off)."""
        self.assertIn("_random_sql_one(", self.fuente)
        self.assertIn("_random_sql_query(", self.fuente)
        self.assertNotIn("pymssql", self.fuente)

    def test_resuelve_el_endo_antes_de_consultar_las_guias(self):
        """Mismo patrón ya confirmado en /api/erp/peek-guias: el ENDO de la
        factura no viene en el querystring/caller -- hay que resolverlo
        primero contra MAEEDO."""
        self.assertIn(
            "SELECT TIDO, NUDO, ENDO FROM MAEEDO WHERE TIDO=%s AND NUDO=%s", self.sql,
        )

    def test_la_query_de_guias_es_la_ya_confirmada_contra_el_erp_real(self):
        """Copia textual de la query probada en /api/erp/peek-guias contra
        BLV 22719 (devolvió GDV 0000032281). No se rediseña."""
        self.assertIn("FROM MAEDDO d", self.sql)
        self.assertIn(
            "LEFT JOIN MAEEDO e ON e.TIDO = d.TIDO AND e.NUDO = d.NUDO AND e.ENDO = d.ENDO",
            self.sql,
        )
        self.assertIn(
            "WHERE d.TIDOPA = %s AND d.NUDOPA = %s AND d.ENDOPA = %s", self.sql,
        )
        self.assertIn("ORDER BY d.NUDO, d.NULIDO", self.sql)

    def test_delete_mas_insert_no_deja_duplicados(self):
        """Resincronizar el mismo documento N veces debe dejar N=1 filas por
        línea de guía, no acumularlas -- mismo patrón que las líneas ZZ en
        _tr_fetch_from_erp (DELETE previo al INSERT, misma transacción)."""
        self.assertIn(
            "DELETE FROM transport_guias WHERE commitment_id=%s", self.sql,
            "Desapareció el DELETE previo al INSERT: el resync podría "
            "duplicar filas de transport_guias.",
        )
        self.assertIn("INSERT INTO transport_guias", self.sql)
        # El DELETE tiene que ejecutarse ANTES del loop de INSERTs, no después.
        idx_delete = self.fuente.index("DELETE FROM transport_guias")
        idx_insert = self.fuente.index("INSERT INTO transport_guias")
        self.assertLess(idx_delete, idx_insert,
                         "El DELETE de transport_guias quedó después del INSERT.")

    def test_guarda_anti_pisado_de_guia_numero(self):
        """Pedido explícito del plan: actualizar transport_commitments.
        guia_numero con la guía MÁS RECIENTE, pero sin poder pisar un valor
        bueno con vacío si esta pasada no trae guía -- mismo criterio que la
        guarda de guia_numero en _tr_fetch_from_erp/_tr_bulk_sync_erp_mysql,
        adaptado a UPDATE directo (esta función no hace UPSERT)."""
        self.assertIn(
            "guia_numero=IF(%sISNULLOR%s='',guia_numero,%s)",
            self.sql_sin_espacios,
            "La guarda anti-pisado de guia_numero desapareció o cambió de forma: "
            "una corrida sin guía nueva podría borrar la guía que ya había.",
        )
        self.assertIn("UPDATE transport_commitments", self.sql)

    def test_elige_la_guia_mas_reciente_por_fecha_guia_descendente(self):
        self.assertIn('key=lambda r: r["fecha_guia"]', self.fuente)
        self.assertIn("max(", self.fuente)

    def test_nunca_propaga_una_excepcion_al_caller(self):
        """100% lectura + escritura local no crítica: un fallo del ERP o del
        pool no configurado no debe poder tumbar al cron horario ni al modal
        que arma el detalle del compromiso."""
        self.assertIn("except Exception as e:", self.fuente)
        self.assertIn("return 0", self.fuente)
        self.assertNotIn("raise", self.fuente)

    def test_no_hace_falta_pool_configurado_para_no_reventar(self):
        """Si _random_sql_one devuelve None (ERP no configurado / sin header),
        la función corta ahí sin intentar nada más."""
        self.assertIn("if not header:", self.fuente)
        self.assertIn("if not rows:", self.fuente)


class TestGuiasEnganchadoAlCron(unittest.TestCase):
    """El cron horario de saldo (tr_cron_refrescar_saldo_productos) es el
    enganche pedido: las guías se sincronizan con la misma cadencia que el
    saldo, documento por documento, sin alterar su alcance/techo existente."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_cron_refrescar_saldo_productos")

    def test_llama_a_fetch_guias_dentro_del_mismo_loop(self):
        self.assertIn("_tr_fetch_guias_from_erp(", self.fuente)

    def test_la_llamada_a_guias_ocurre_solo_si_el_saldo_se_actualizo(self):
        """Tiene que colgar del mismo comm_id ya resuelto por
        _tr_fetch_from_erp -- no una consulta nueva/independiente."""
        idx_saldo = self.fuente.index('comm_id, err = _tr_fetch_from_erp(')
        idx_guias = self.fuente.index("_tr_fetch_guias_from_erp(")
        self.assertLess(
            idx_saldo, idx_guias,
            "La llamada a guías quedó antes de resolver comm_id vía "
            "_tr_fetch_from_erp: comm_id no existiría todavía.",
        )
        self.assertIn("_tr_fetch_guias_from_erp(comm_id,", self.fuente)

    def test_un_fallo_de_guias_no_cuenta_como_fallo_de_saldo(self):
        """El cron reporta actualizados/fallidos sobre el refresco de SALDO
        -- un problema sincronizando guías no debe inflar `fallidos` ni
        opacar si el saldo sí se actualizó bien. Se verifica por posición:
        un `try:` justo antes de la llamada y un `except ... as e_g:` propio
        justo después -- distinto del `except Exception as e:` del loop
        general (ese es el que cuenta fallidos del saldo)."""
        llamada = '_tr_fetch_guias_from_erp(comm_id, d["tido"], str(d["nudo"]))'
        self.assertIn(llamada, self.fuente)
        idx_llamada = self.fuente.index(llamada)
        antes = self.fuente[:idx_llamada]
        despues = self.fuente[idx_llamada + len(llamada):]
        self.assertIn("try:", antes[-40:],
                       "No hay un try: propio justo antes de la llamada a guías.")
        self.assertIn("except Exception as e_g:", despues[:60],
                       "No hay un except propio (as e_g) justo después de la "
                       "llamada a guías -- podría estar cayendo en el except "
                       "general del loop y contando como fallo de saldo.")

    def test_no_toco_el_alcance_ni_el_techo_existentes_del_cron(self):
        """No debía cambiar el resto de la lógica del cron: mismo filtro de
        saldo, mismos estados terminales excluidos, mismo LIMIT."""
        self.assertIn("c.tiene_saldo = 1", self.fuente)
        self.assertIn("estado_entrega NOT IN", self.fuente)
        self.assertRegex(_norm(self.fuente), r"LIMIT \d+")


class TestGuiasEnElModalDeVista(unittest.TestCase):
    """`tr_detalle` (endpoint /transporte/api/compromisos/<cid>/detalle) es la
    fuente real de la tabla de productos del modal "Vista previa"
    (vistaTabla/vistaTbody en templates/transporte/index.html +
    static/transporte_monitor.js) -- NO _tr_buscar_detalle, que alimenta el
    modal de tracking (documentos ya con manifiesto). Ahí es donde Daniel
    pidió la columna "Guía" visible."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_detalle")

    def test_cruza_transport_guias_por_sku_para_cada_linea_de_producto(self):
        self.assertIn("FROM transport_guias", self.fuente)
        self.assertIn("WHERE commitment_id=%s", _norm(self.fuente))
        self.assertIn('_l["guia_numero"]', self.fuente)

    def test_usa_la_guia_mas_reciente_por_fecha_guia_descendente(self):
        self.assertIn("ORDER BY fecha_guia DESC", self.fuente)

    def test_no_es_critico_si_falla(self):
        """El cruce de guías es puramente informativo -- si falla, el modal
        debe seguir funcionando (guia_numero queda en None → "—" en la UI),
        nunca tumbar el detalle completo del documento."""
        self.assertIn("except Exception as _e_guia:", self.fuente)


class TestGuiasColumnaSoloEnModalNoEnGrillaPrincipal(unittest.TestCase):
    """Corrección explícita de alcance de Daniel: la columna "Guía" va SOLO
    en la tabla de productos del modal (vistaTabla), nunca como columna nueva
    en la grilla principal del Monitor (la lista de documentos con
    Documento/Fecha/Cliente/Estado/Courier/etc) -- un documento puede tener
    varias guías (multi-despacho parcial), así que una sola columna ahí
    sería confusa."""

    def test_la_columna_guia_esta_en_vistatabla(self):
        ruta = os.path.join(RAIZ, "templates", "transporte", "index.html")
        with open(ruta, encoding="utf-8") as fh:
            html = fh.read()
        idx_vistatabla = html.index('id="vistaTabla"')
        idx_th_guia = html.index('<th class="text-center">Guía</th>')
        # La grilla principal del Monitor se define ANTES que el modal de
        # vista previa en este template -- si el <th> de Guía apareciera
        # antes de vistaTabla, sería la señal de que se coló en la grilla
        # principal en vez de quedar dentro del modal.
        self.assertGreater(
            idx_th_guia, idx_vistatabla,
            "El <th> de Guía aparece ANTES de #vistaTabla -- revisar que no "
            "se haya agregado a la grilla principal del Monitor por error.",
        )

    def test_la_grilla_principal_no_tiene_columna_guia(self):
        """La grilla principal del Monitor vive en un <table> distinto de
        vistaTabla (fuera del modal). Verificamos que ese bloque específico
        no contenga la columna nueva."""
        ruta = os.path.join(RAIZ, "templates", "transporte", "index.html")
        with open(ruta, encoding="utf-8") as fh:
            html = fh.read()
        idx_vistatabla = html.index('id="vistaTabla"')
        grilla_principal = html[:idx_vistatabla]
        self.assertNotIn(
            '<th class="text-center">Guía</th>', grilla_principal,
            "Apareció una columna 'Guía' antes del modal vistaTabla -- "
            "Daniel pidió explícitamente que la grilla principal del "
            "Monitor NO tenga esa columna (multi-despacho parcial la haría "
            "confusa ahí).",
        )


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 10 · Preventa automática por producto, derivada del stock real
# ═════════════════════════════════════════════════════════════════════════════
class TestPreventaAutomaticaPorProducto(unittest.TestCase):
    """Daniel, 2026-08-01: "que se vaya actualizando... como relojito" —
    cuando el ERP no tiene stock de un producto, el badge de estado debe decir
    'Preventa' SOLO, sin que nadie lo marque a mano. Es derivado en cada
    lectura (no un valor que se escribe): apenas vuelve a haber stock, deja de
    verse 'Preventa' sin que nadie tenga que revertir nada."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_tr_buscar_detalle")
        cls.norm = _norm(cls.fuente)

    def test_la_regla_de_auto_preventa_existe(self):
        self.assertIn('_lo["estado_efectivo"] = "Preventa"', self.fuente)
        self.assertIn("es_auto_preventa", self.fuente)

    def test_nunca_gana_sobre_un_estado_puesto_a_mano(self):
        """Un estado_linea explícito (alguien lo marcó en el selector) tiene
        que seguir ganando siempre — la auto-preventa solo llena el vacío
        cuando NADIE lo tocó."""
        self.assertRegex(
            self.norm,
            r'if _explicito: _lo\["estado_efectivo"\] = _t\.get\("estado_linea"\)',
            "El orden cambió: un estado puesto a mano debe evaluarse ANTES "
            "que la auto-preventa, nunca después.",
        )

    def test_no_aplica_si_el_despacho_ya_es_terminal(self):
        """No tiene sentido decir 'Preventa' de un producto que ya se
        entregó o devolvió solo porque el stock GLOBAL bajó después."""
        self.assertIn("_estado_despacho not in ESTADOS_ENTREGA_TERMINALES", self.norm)

    def test_no_aplica_si_esta_linea_ya_no_tiene_saldo_pendiente(self):
        """Si esta línea puntual del documento ya se despachó completa, el
        stock global en 0 no la vuelve 'Preventa' retroactivamente."""
        self.assertIn('_lo.get("saldo", 0) > 0', self.norm)

    def test_no_aplica_si_no_hay_dato_de_stock(self):
        """stock_disponible None (el sondeo de stock no alcanzó a este SKU)
        no debe interpretarse como 'sin stock' — sería un falso positivo."""
        self.assertIn('_lo.get("stock_disponible") is not None', self.norm)


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 11 · Fase 1 Paso 3 (PR-B, 2026-08-01) — motor de decisión compartido
# ═════════════════════════════════════════════════════════════════════════════
class TestPlanRamoManifestItem(unittest.TestCase):
    """_tr_plan_ramo_manifest_item es el corazón de PR-B: función PURA (sin
    MySQL) que decide, para un documento con N ramos, si corresponde
    insertar/no-opear/rechazar un manifest_item en un manifiesto destino.
    Los 4 write-sites son wrappers finos alrededor de esta decisión -- se
    prueba EXHAUSTIVAMENTE acá porque es mucho más barato que probar cada
    write-site por separado, y porque es la pieza que implementa la decisión
    de Daniel (2026-07-31): dos ramos del MISMO documento no pueden ir al
    MISMO manifiesto."""

    @classmethod
    def setUpClass(cls):
        cls.plan = staticmethod(_extraer_funcion("_tr_plan_ramo_manifest_item"))

    def test_documento_de_un_solo_ramo_fresco_inserta(self):
        """Caso de HOY, la inmensa mayoría: 1 ramo, nada asignado todavía."""
        r = self.plan(["despacho"], [], mid=5)
        self.assertEqual(r, {"accion": "insertar", "ramo": "despacho"})

    def test_documento_de_un_solo_ramo_ya_en_el_mismo_manifiesto_no_opera(self):
        items = [{"ramo": "despacho", "manifest_id": 5}]
        r = self.plan(["despacho"], items, mid=5)
        self.assertEqual(r, {"accion": "ya_estaba", "ramo": "despacho"})

    def test_documento_de_un_solo_ramo_en_otro_manifiesto_es_duplicado_legacy(self):
        """Mismo comportamiento preexistente (confirm_dup) que ya cubría
        TestFacturaEnDosManifiestos -- se preserva intacto para 1 ramo."""
        items = [{"ramo": "despacho", "manifest_id": 6}]
        r = self.plan(["despacho"], items, mid=5)
        self.assertEqual(r, {"accion": "duplicado", "ramo": "despacho", "otro_manifest_id": 6})

    def test_multi_ramo_fresco_sin_desambiguar_es_ambiguo(self):
        """El caso central que el modelo de dos ramos tiene que resolver: SIN
        una forma de saber cuál ramo se quiere (ramo_solicitado=None), 2+
        ramos pendientes no se pueden asignar solos -- es exactamente la
        situación que hoy (antes de PR-B) colapsaba en silencio a un solo
        ramo, perdiendo el otro."""
        r = self.plan(["despacho", "instalacion"], [], mid=5)
        self.assertEqual(r, {"accion": "conflicto_multi_ramo", "pendientes": ["despacho", "instalacion"]})

    def test_multi_ramo_con_ramo_solicitado_explicito_resuelve_sin_ambiguedad(self):
        """Cuando el caller SÍ sabe qué ramo quiere (ej. lineas-pendientes,
        que lo deriva del SKU de la línea seleccionada), no hay ambigüedad
        aunque el documento tenga 2+ ramos -- esto es lo que permite que el
        PRIMER ramo de un documento multi-ramo se pueda asignar."""
        r = self.plan(["despacho", "instalacion"], [], mid=5, ramo_solicitado="despacho")
        self.assertEqual(r, {"accion": "insertar", "ramo": "despacho"})

    def test_completar_el_segundo_ramo_en_otro_manifiesto_no_tiene_conflicto(self):
        """El flujo NUEVO que cierra el gap real: despacho ya fue asignado
        (a OTRO manifiesto, en una operación previa) -- ahora se completa
        instalación, sin especificar ramo (ej. desde tr_agregar_item en la
        ficha), y el único ramo pendiente se resuelve solo."""
        items = [{"ramo": "despacho", "manifest_id": 6}]
        r = self.plan(["despacho", "instalacion"], items, mid=5)
        self.assertEqual(r, {"accion": "insertar", "ramo": "instalacion"})

    def test_segundo_ramo_en_el_mismo_manifiesto_ya_no_es_conflicto(self):
        """CORRECCIÓN 2026-08-01 (Daniel, en vivo -- "despachamos e instalamos
        [juntos]"): la decisión original del 2026-07-31 (bloquear 2 ramos del
        mismo documento en el mismo manifiesto) era incorrecta -- despacho +
        instalación normalmente van en el MISMO viaje. Agregar el segundo
        ramo al manifiesto que ya tiene el primero debe insertar sin más."""
        items = [{"ramo": "despacho", "manifest_id": 5}]
        r = self.plan(["despacho", "instalacion"], items, mid=5)
        self.assertEqual(r, {"accion": "insertar", "ramo": "instalacion"})

    def test_segundo_ramo_con_ramo_solicitado_explicito_tambien_inserta(self):
        items = [{"ramo": "despacho", "manifest_id": 5}]
        r = self.plan(["despacho", "instalacion"], items, mid=5, ramo_solicitado="instalacion")
        self.assertEqual(r, {"accion": "insertar", "ramo": "instalacion"})

    def test_ramo_solicitado_invalido_para_el_documento_se_ignora_no_se_inventa(self):
        """Defensivo: si ramo_solicitado no está entre los ramos reales del
        documento (no debería pasar en la práctica), se trata como si no se
        hubiera mandado -- nunca se inventa un ramo que el documento no tiene."""
        r = self.plan(["despacho"], [], mid=5, ramo_solicitado="instalacion")
        self.assertEqual(r, {"accion": "insertar", "ramo": "despacho"})

    def test_documento_completamente_resuelto_usa_el_primer_ramo_como_referencia(self):
        """Si TODOS los ramos ya tienen item en algún lado (documento 100%
        resuelto) y se llama sin ramo_solicitado, no hay nada pendiente que
        insertar -- se usa el primer ramo como referencia para el chequeo de
        duplicado/ya_estaba (mismo criterio legacy de siempre para 1 ramo)."""
        items = [{"ramo": "despacho", "manifest_id": 6}, {"ramo": "instalacion", "manifest_id": 7}]
        r = self.plan(["despacho", "instalacion"], items, mid=8)
        self.assertEqual(r, {"accion": "duplicado", "ramo": "despacho", "otro_manifest_id": 6})

    def test_lista_de_ramos_vacia_cae_a_despacho_por_defecto(self):
        """Defensivo: _tr_ramos_de_commitment nunca debería devolver [], pero
        si pasara, no debe reventar -- cae al mismo default que el resto del
        sistema."""
        r = self.plan([], [], mid=5)
        self.assertEqual(r, {"accion": "insertar", "ramo": "despacho"})

    def test_tres_ramos_solo_uno_pendiente_no_es_ambiguo(self):
        items = [{"ramo": "despacho", "manifest_id": 1}, {"ramo": "retiro", "manifest_id": 2}]
        r = self.plan(["despacho", "retiro", "instalacion"], items, mid=3)
        self.assertEqual(r, {"accion": "insertar", "ramo": "instalacion"})


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 11.b · Fase 1 (PR-A -> PR-B, 2026-08-01) — los 4 write-sites usan el
# modelo de ramos múltiples en vez de la `clasificacion` singular
# ═════════════════════════════════════════════════════════════════════════════
class TestWriteSitesRamoMultiple(unittest.TestCase):
    """PR-A (paso anterior) dejaba los 4 write-sites leyendo
    transport_commitments.clasificacion (1 valor por documento) para setear
    `ramo` -- funcional, pero es exactamente lo que perdía el segundo ramo de
    un documento (ver TestClasificacionZZ). PR-B los migra a
    _tr_ramos_de_commitment + _tr_plan_ramo_manifest_item (BLOQUE 11 arriba),
    que sí puede crear más de un manifest_item por documento, cada uno con su
    propio ramo, respetando el candado de Daniel."""

    FUNCIONES = [
        "tr_lineas_pendientes_enviar_manifiesto",
        "tr_agregar_item",
        "tr_asignar_a_manifiesto",
        "tr_cubicador_enviar_manifiesto",
    ]
    # tr_lineas_pendientes_enviar_manifiesto SIEMPRE resuelve un
    # ramo_solicitado explícito (granularidad de línea, ver su propio bloque
    # más abajo) -- por diseño nunca llega a la rama ambigua del planner, así
    # que es el único de los 4 que no referencia "conflicto_multi_ramo".
    FUNCIONES_CON_CONFLICTO_MULTI_RAMO = [
        "tr_agregar_item", "tr_asignar_a_manifiesto", "tr_cubicador_enviar_manifiesto",
    ]

    def test_los_cuatro_sitios_incluyen_ramo_en_el_insert(self):
        for nombre in self.FUNCIONES:
            cuerpo = _norm(_cuerpo_funcion(nombre))
            with self.subTest(funcion=nombre):
                self.assertIn(
                    "INSERT IGNORE INTO transport_manifest_items", cuerpo,
                    f"{nombre} ya no hace el INSERT esperado — revisar si cambió de función.",
                )
                self.assertRegex(
                    cuerpo,
                    # [\s"]* en vez de \s*: el SQL es una concatenación de
                    # strings de Python ("...items " "(manifest_id..."), y
                    # _cuerpo_funcion devuelve el código FUENTE tal cual está
                    # escrito -- comillas de concatenación incluidas.
                    r'INSERT IGNORE INTO transport_manifest_items[\s"]*\([^)]*\bramo\b[^)]*\)',
                    f"{nombre} volvió a insertar sin `ramo` explícito — los items nuevos "
                    "quedarían NULL otra vez, rompiendo la precondición del endurecimiento "
                    "del UNIQUE KEY.",
                )

    def test_los_cuatro_sitios_usan_el_motor_compartido_de_ramos_multiples(self):
        """PR-B: ya no leen `clasificacion` (singular) para decidir el ramo
        del INSERT -- delegan en _tr_ramos_de_commitment (lista completa de
        ramos del documento) + _tr_plan_ramo_manifest_item (la decisión)."""
        for nombre in self.FUNCIONES:
            cuerpo = _cuerpo_funcion(nombre)
            with self.subTest(funcion=nombre):
                self.assertIn("_tr_ramos_de_commitment(", cuerpo,
                             f"{nombre} ya no resuelve los ramos del documento vía "
                             "_tr_ramos_de_commitment.")
                self.assertIn("_tr_plan_ramo_manifest_item(", cuerpo,
                             f"{nombre} ya no usa el motor de decisión compartido "
                             "_tr_plan_ramo_manifest_item.")

    def test_ningun_sitio_maneja_ya_el_candado_retirado(self):
        """CORRECCIÓN 2026-08-01: el candado 'conflicto_candado' (bloquear 2
        ramos del mismo documento en el mismo manifiesto) se retiró -- ya no
        debe quedar código muerto referenciándolo en ninguno de los 4
        write-sites."""
        for nombre in self.FUNCIONES:
            cuerpo = _norm(_cuerpo_funcion(nombre))
            with self.subTest(funcion=nombre):
                self.assertNotIn("conflicto_candado", cuerpo,
                             f"{nombre} todavía referencia 'conflicto_candado' -- "
                             "código muerto del candado retirado, limpiar.")

    def test_tres_sitios_manejan_ambiguedad_multi_ramo_con_409(self):
        """Los 3 write-sites SIN granularidad de línea (documento completo)
        deben rechazar con 409 cuando un documento tiene 2+ ramos sin asignar
        y no hay forma de saber cuál se quiere -- ver la excepción documentada
        de tr_lineas_pendientes_enviar_manifiesto en FUNCIONES_CON_CONFLICTO_MULTI_RAMO."""
        for nombre in self.FUNCIONES_CON_CONFLICTO_MULTI_RAMO:
            cuerpo = _norm(_cuerpo_funcion(nombre))
            with self.subTest(funcion=nombre):
                self.assertIn("conflicto_multi_ramo", cuerpo,
                             f"{nombre} ya no maneja el resultado 'conflicto_multi_ramo' del planner.")
                self.assertRegex(
                    cuerpo,
                    r'conflicto_multi_ramo.{0,400}?\), 409',
                    f"{nombre} maneja 'conflicto_multi_ramo' pero no devuelve 409.",
                )

    def test_ningun_sitio_deja_ramo_sin_fallback(self):
        """Si el ramo resuelto viniera vacío por algún motivo, ninguno de los
        4 sitios debe insertar ramo=NULL — todos tienen que caer a
        'despacho' como default seguro (defensa en profundidad, aunque
        _tr_ramos_de_commitment / _tr_plan_ramo_manifest_item ya garantizan
        que esto no debería ocurrir en la práctica)."""
        for nombre in self.FUNCIONES:
            cuerpo = _cuerpo_funcion(nombre)
            with self.subTest(funcion=nombre):
                self.assertIn('"despacho"', cuerpo,
                             f"{nombre} perdió el fallback a 'despacho'.")

    def test_tr_lineas_pendientes_resuelve_ramo_por_linea_no_por_documento_completo(self):
        """La pieza que distingue a este write-site de los otros 3: puede
        derivar el ramo EXACTO de la línea seleccionada (vía el SKU real,
        ej. 'ZZENVIO' vs 'ZZINSTALACION') en vez de mirar el documento
        completo -- eso es lo que le permite resolver el PRIMER ramo de un
        documento fresco con 2+ ramos, algo que los otros 3 write-sites (sin
        esa granularidad) no pueden hacer solos."""
        cuerpo = _cuerpo_funcion("tr_lineas_pendientes_enviar_manifiesto")
        self.assertIn("_clasif_from_skus([s])", cuerpo,
                     "Dejó de derivar el ramo por línea con _clasif_from_skus([sku]) -- "
                     "revisar si todavía puede resolver el primer ramo de un documento "
                     "multi-ramo fresco.")
        self.assertIn("_ramos_solicitados_por_cid", _norm(cuerpo))

    def test_tr_lineas_pendientes_acepta_dos_ramos_del_mismo_documento_en_el_mismo_pedido(self):
        """CORRECCIÓN 2026-08-01: si el operador selecciona líneas de 2 ramos
        distintos de la MISMA factura para el MISMO envío (ej. la línea
        ZZENVIO y la línea ZZINSTALACION), eso ya NO se rechaza -- en la
        operación real, despacho e instalación normalmente van juntos.
        _ramos_solicitados_por_cid guarda la LISTA completa (no un único
        ramo) y el código arma un plan+insert por cada (cid, ramo)."""
        cuerpo = _norm(_cuerpo_funcion("tr_lineas_pendientes_enviar_manifiesto"))
        self.assertIn("_ramos_solicitados_por_cid", cuerpo,
                     "Dejó de acumular la lista de ramos solicitados por documento.")
        self.assertNotIn("_conflictos_mismo_pedido", cuerpo,
                     "El bloqueo _conflictos_mismo_pedido (candado retirado) sigue presente.")

    def test_tr_agregar_item_y_tr_asignar_siguen_preservando_confirm_dup(self):
        """El mecanismo LEGACY preexistente (confirm_dup) para el caso
        distinto -- mismo ramo en OTRO manifiesto -- sigue siendo una
        decisión legítima del operador (ej. redespacho parcial), no un error."""
        for nombre in ("tr_agregar_item", "tr_asignar_a_manifiesto"):
            cuerpo = _cuerpo_funcion(nombre)
            with self.subTest(funcion=nombre):
                self.assertIn("confirm_dup", cuerpo,
                             f"{nombre} perdió el escape confirm_dup para duplicados legítimos.")


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 11.c · Mensajes de conflicto de ramo (PR-B, 2026-08-01)
# ═════════════════════════════════════════════════════════════════════════════
class TestRespuestasConflictoRamo(unittest.TestCase):
    """_tr_resp_conflicto_multi_ramo arma el payload 409 legible que ven los
    write-sites -- se prueba ejecutando la función real (es pura: dict in,
    dict out). _tr_resp_conflicto_candado se ELIMINÓ en la corrección
    2026-08-01 (el candado que bloqueaba 2 ramos en el mismo manifiesto ya
    no existe)."""

    @classmethod
    def setUpClass(cls):
        _ramo_labels = None
        for nodo in ast.walk(_arbol_app()):
            if isinstance(nodo, ast.Assign):
                for destino in nodo.targets:
                    if isinstance(destino, ast.Name) and destino.id == "_RAMO_LABELS":
                        _ramo_labels = ast.literal_eval(nodo.value)
        if _ramo_labels is None:
            raise AssertionError("No se encontró _RAMO_LABELS en app.py")
        _label_fn = _extraer_funcion("_tr_ramo_label", extras={"_RAMO_LABELS": _ramo_labels})
        cls.multi = staticmethod(
            _extraer_funcion("_tr_resp_conflicto_multi_ramo", extras={"_tr_ramo_label": _label_fn}))

    def test_conflicto_multi_ramo_lista_todos_los_pendientes(self):
        plan = {"accion": "conflicto_multi_ramo", "pendientes": ["despacho", "instalacion"]}
        r = self.multi(plan, "BLV 999")
        self.assertEqual(r["error"], "ramo_ambiguo")
        self.assertIn("despacho", r["msg"])
        self.assertIn("instalación", r["msg"])
        self.assertIn("Líneas pendientes", r["msg"])
        self.assertEqual(r["ramos_pendientes"], ["despacho", "instalacion"])


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 11.d · Grilla del Monitor: 1 fila por ramo con manifest_item real
# (PR-B, 2026-08-01)
# ═════════════════════════════════════════════════════════════════════════════
class TestCompromisosJsonExpansionPorRamo(unittest.TestCase):
    """tr_compromisos_json necesita MySQL real para ejecutarse -- se prueba
    ESTRUCTURALMENTE (mismo patrón que el resto del archivo para funciones
    con acceso a BD): que arme el fetch batch de items reales y expanda la
    fila cuando corresponde, sin tocar el WHERE de pendiente/en_gestion/
    entregado ni los `conteos` (decisión de diseño documentada en el propio
    código -- ver comentario en tr_compromisos_json)."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_compromisos_json")
        cls.norm = _norm(cls.fuente)

    def test_hace_un_fetch_batch_de_items_reales_por_ramo(self):
        self.assertIn("_items_por_commitment", self.fuente,
                     "Desapareció el fetch batch de items reales -- sin esto no hay "
                     "forma de expandir en N filas.")
        self.assertRegex(
            self.norm,
            r"SELECT mi\.commitment_id, mi\.ramo, mi\.estado_entrega, mi\.manifest_id,"
            r"[\s\"]*tm\.courier, tm\.correlativo",
            "Cambió la forma del fetch batch de manifest_items -- revisar que siga "
            "trayendo ramo/estado/manifest_id/courier/correlativo.",
        )

    def test_el_fetch_batch_ordena_por_id_desc_igual_que_las_subqueries_de_arriba(self):
        """Mismo criterio "más reciente primero" que _COURIER_SUB/_MANIFIESTO_ID_SUB/
        etc (ORDER BY tmi.id DESC LIMIT 1) -- así el item[0] de cada commitment
        coincide exactamente con lo que la fila base YA trae, y no hace falta
        recalcularlo para el caso de 1 item (la inmensa mayoría)."""
        self.assertIn("ORDER BY mi.commitment_id, mi.id DESC", self.norm)

    def test_documento_sin_items_sigue_siendo_una_sola_fila(self):
        """Caso de HOY sin cambios: 0 manifest_items -> 1 fila, sin expandir."""
        self.assertIn("if len(_items_r) <= 1:", self.fuente)

    def test_cada_fila_expandida_tiene_su_propio_ramo(self):
        self.assertRegex(self.norm, r'_fila\["ramo"\]\s*=\s*_it\.get\("ramo"\)')

    def test_filas_expandidas_mas_alla_de_la_primera_recalculan_courier_manifiesto_y_estado(self):
        """El item[0] ya coincide con la fila base (mismo "más reciente") --
        solo las filas 2+ necesitan pisar courier/manifiesto/estado con SU
        PROPIO item, no el del item más reciente del documento completo."""
        for campo in ('_fila["courier"]', '_fila["manifiesto_id"]',
                      '_fila["manifiesto_correlativo"]', '_fila["gestion"]',
                      '_fila["estado_logistico"]'):
            self.assertIn(campo, self.fuente,
                         f"Falta recalcular {campo} para las filas expandidas 2+.")

    def test_dias_atraso_de_una_fila_con_item_real_siempre_es_cero(self):
        """gestion=pendiente (el único caso con dias_atraso>0) nunca aplica a
        una fila con manifest_item real -- en_manif=1 siempre cae a
        en_gestion o entregado."""
        self.assertIn('_fila["dias_atraso"] = 0', self.fuente)

    def test_no_toca_el_where_de_pendiente_en_gestion_entregado(self):
        """Decisión de diseño documentada: la partición pendiente/en_gestion/
        entregado sigue siendo por DOCUMENTO -- reescribirla a nivel de ramo
        es un cambio de alcance mayor, fuera de esta fase. Este test fija que
        las 3 macros SQL siguen ahí, sin que la expansión las haya tocado."""
        for macro in ("_SQL_PENDIENTE", "_SQL_ENGESTION", "_SQL_ENTREGADO"):
            self.assertIn(macro, self.fuente)

    def test_conteos_siguen_siendo_consultas_agregadas_directas_sobre_transport_commitments(self):
        """Mismo criterio: `conteos` no se computó a partir de `result` (que
        sí puede tener más filas que documentos) -- sigue habiendo varias
        queries SQL independientes de COUNT/SUM sobre transport_commitments
        (conteos_row/_eg_row/_ent_row/_pend_row), exactamente como antes de
        PR-B, y ninguna de las claves de `conteos` se deriva de `len(result)`
        ni de iterar `result`."""
        # Al menos las 4 queries agregadas preexistentes (conteos_row +
        # en_gestion + entregados + pendientes) siguen apuntando directo a
        # la tabla, no a la lista `result` ya armada en Python.
        self.assertGreaterEqual(self.norm.count("FROM transport_commitments WHERE"), 4)
        idx_conteos = self.fuente.index('"conteos": {')
        idx_fin = self.fuente.index("})", idx_conteos)
        bloque_conteos = self.fuente[idx_conteos:idx_fin]
        self.assertNotIn("result", bloque_conteos,
                         "El armado de `conteos` en la respuesta empezó a referenciar "
                         "`result` -- eso significaría que se migró a contar filas "
                         "expandidas en vez de documentos (cambio de diseño no documentado).")


class TestUqItemEndurecido(unittest.TestCase):
    """El UNIQUE KEY (manifest_id, commitment_id) impide que un documento
    tenga 2 manifest_items (uno por ramo) en el MISMO manifiesto — el segundo
    INSERT sería un no-op silencioso. Se endurece a (manifest_id,
    commitment_id, ramo), pero SOLO tras confirmar cero filas ramo=NULL,
    porque MySQL permite múltiples NULL en una columna de índice único: si el
    ALTER corriera antes, el índice quedaría "puesto" sin proteger nada."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_ensure_transport_tracking_tables")
        cls.norm = _norm(cls.fuente)

    def test_verifica_cero_null_antes_de_endurecer_no_lo_asume(self):
        self.assertIn(
            "SELECT COUNT(*) AS n FROM transport_manifest_items WHERE ramo IS NULL",
            self.norm,
            "Desapareció la verificación explícita — el endurecimiento no "
            "puede asumir que el backfill ya corrió, tiene que confirmarlo.",
        )

    def test_el_modify_not_null_ocurre_solo_si_el_count_dio_cero(self):
        self.assertRegex(
            self.norm,
            r'if \(_pendientes_ramo or \{\}\)\.get\("n", 1\) == 0: mysql_execute\("""\s*'
            r'ALTER TABLE transport_manifest_items\s*MODIFY COLUMN ramo',
            "El MODIFY COLUMN a NOT NULL ya no está condicionado al COUNT()==0 — "
            "podría ejecutarse con filas NULL todavía presentes.",
        )

    def test_el_orden_es_modify_not_null_antes_que_el_indice(self):
        """Invertir el orden (índice antes que NOT NULL) no rompe nada
        técnicamente, pero el objetivo es documentar la secuencia esperada:
        si algún día alguien reordena esto sin querer, este test lo marca."""
        idx_modify = self.fuente.index("MODIFY COLUMN ramo")
        idx_drop_index = self.fuente.index("DROP INDEX uq_item")
        self.assertLess(
            idx_modify, idx_drop_index,
            "El MODIFY COLUMN (NOT NULL) debe ir ANTES del DROP/ADD INDEX en el código.",
        )

    def test_reemplaza_el_indice_via_drop_mas_add_no_modify(self):
        """No se puede renombrar/redefinir un UNIQUE KEY existente con un solo
        ALTER portable — tiene que ser DROP seguido de ADD."""
        self.assertIn("ALTER TABLE transport_manifest_items DROP INDEX uq_item", self.norm)
        self.assertIn(
            "ADD UNIQUE KEY uq_item (manifest_id, commitment_id, ramo)", self.norm,
        )

    def test_el_drop_del_indice_esta_en_su_propio_try_except(self):
        """Si uq_item ya fue reemplazado en un boot anterior, el DROP falla
        (el índice viejo ya no existe con ese nombre) — eso NO debe abortar
        el ADD que le sigue. Verificado por estructura: debe haber un
        try/except envolviendo el DROP, separado del que envuelve el ADD."""
        # Extrae el fragmento entre "DROP INDEX uq_item" y "ADD UNIQUE KEY uq_item"
        idx_drop = self.fuente.index("DROP INDEX uq_item")
        idx_add = self.fuente.index("ADD UNIQUE KEY uq_item")
        fragmento_entre = self.fuente[idx_drop:idx_add]
        self.assertIn(
            "except Exception:", fragmento_entre,
            "El DROP INDEX ya no tiene su propio except — un boot donde el "
            "índice ya fue reemplazado antes abortaría el ADD que le sigue.",
        )

    def test_si_quedan_filas_null_no_ejecuta_ningun_alter(self):
        """El caso "todavía no": ninguna sentencia ALTER debe correr si el
        COUNT() no dio cero — solo se loguea y se reintenta en el próximo boot."""
        self.assertIn("se reintenta en el próximo boot", self.fuente)


class TestFusionDuplicados(unittest.TestCase):
    """2026-08-01 (Daniel: "borrémoslo, a menos que no esté en un
    manifiesto"): _tr_fusionar_un_grupo nunca debe borrar una fila que
    tenga manifest_items reales, nunca debe perder evidencia/financiero
    huérfano, y nunca debe pisar un dato que el ganador ya tenga."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("_tr_fusionar_un_grupo")
        cls.norm = _norm(cls.fuente)

    def test_rechaza_perdedor_con_manifest_items(self):
        self.assertRegex(
            self.norm,
            r'SELECT COUNT\(\*\)[\s"]*AS n FROM transport_manifest_items[\s"]*WHERE commitment_id=%s',
            "Falta la verificación de que el perdedor no tenga manifest_items "
            "antes de intentar fusionarlo.",
        )
        self.assertIn('raise ValueError(f"id={loser_id} tiene manifest_items', self.fuente)

    def test_verificacion_de_manifest_items_ocurre_antes_del_delete(self):
        idx_check = self.fuente.index("manifest_items WHERE commitment_id=%s")
        idx_delete = self.fuente.index("DELETE FROM transport_commitments")
        self.assertLess(
            idx_check, idx_delete,
            "La verificación de manifest_items debe ocurrir ANTES del DELETE, "
            "no después — si no, el DELETE podría correr sobre una fila insegura.",
        )

    def test_detecta_conflicto_financiero_de_ambos_lados(self):
        self.assertIn("transport_factura_proveedor_items", self.fuente)
        self.assertIn("requiere revisión manual", self.norm)

    def test_repunta_las_cuatro_tablas_sin_fk_real_antes_de_borrar(self):
        """transport_tracking_events, transport_delivery_proof,
        transport_item_lines y transport_factura_proveedor_items NO tienen
        FOREIGN KEY hacia transport_commitments -- si no se repuntan antes
        del DELETE, quedan huérfanas en silencio (id apuntando a nada)."""
        idx_delete = self.fuente.index("DELETE FROM transport_commitments")
        antes_del_delete = self.fuente[:idx_delete]
        for tabla in ("transport_tracking_events", "transport_delivery_proof",
                      "transport_item_lines", "transport_factura_proveedor_items"):
            self.assertIn(
                tabla, antes_del_delete,
                f"{tabla} no se repunta antes del DELETE -- quedaría huérfana.",
            )

    def test_repunta_guias_antes_de_borrar_pese_a_tener_cascade(self):
        """transport_guias SÍ tiene ON DELETE CASCADE -- sin repuntarla
        explícitamente, el DELETE la borraría junto con el perdedor en vez
        de conservarla en el ganador."""
        idx_delete = self.fuente.index("DELETE FROM transport_commitments")
        self.assertIn("transport_guias", self.fuente[:idx_delete])

    def test_fusion_de_campos_solo_llena_huecos_del_ganador(self):
        """No debe existir ningún UPDATE que pise un campo del ganador
        incondicionalmente -- el fill-if-empty se arma dinámicamente
        (sets/vals) solo para campos donde el ganador viene vacío."""
        self.assertIn("_w_vacio and _l_tiene", self.norm)
        self.assertIn("campos_llenados", self.fuente)

    def test_no_toca_campos_de_estado_clasificacion_o_financieros(self):
        """La fusión de campos es deliberadamente una whitelist chica --
        nunca debe incluir columnas de estado/dinero que reflejen lógica
        de la app, esas deben venir tal cual del ganador."""
        idx_whitelist = self.fuente.index("CAMPOS_FUSIONABLES = (")
        idx_fin = self.fuente.index(")", idx_whitelist)
        whitelist_src = self.fuente[idx_whitelist:idx_fin]
        for campo_prohibido in ("estado", "clasificacion", "tiene_saldo",
                                 "valor_neto", "valor_bruto", "costo_zz"):
            self.assertNotIn(
                f'"{campo_prohibido}"', whitelist_src,
                f"'{campo_prohibido}' no debe estar en la whitelist de fusión "
                f"-- es un campo de estado/dinero que debe venir del ganador tal cual.",
            )

    def test_audit_log_ocurre_antes_del_delete(self):
        """REGLA #5: audit log en TODA acción destructiva, ANTES de borrar."""
        idx_log = self.fuente.index('_tr_log(\n        "commitment_merge"')
        idx_delete = self.fuente.index("DELETE FROM transport_commitments")
        self.assertLess(idx_log, idx_delete,
                         "El _tr_log de auditoría debe ir antes del DELETE, no después.")

    def test_snapshot_del_perdedor_va_en_el_log(self):
        """Si algún día hay que reconstruir manualmente un merge, el log
        debe traer el snapshot completo de la fila borrada, no solo su id."""
        self.assertIn("Snapshot perdedor:", self.fuente)
        self.assertIn("json.dumps(loser_row", self.fuente)


class TestFusionarEndpointUsaMismoDesempateQueElDiagnostico(unittest.TestCase):
    """El endpoint de fusión (POST) y el de diagnóstico (GET, solo lectura)
    deben agrupar y desempatar EXACTAMENTE igual -- si divergen, el reporte
    que ve Daniel ("esta fila ganaría") ya no predice qué hace la fusión real."""

    @classmethod
    def setUpClass(cls):
        cls.diag = _cuerpo_funcion("tr_diagnostico_duplicados")
        cls.fusion = _cuerpo_funcion("tr_diagnostico_duplicados_fusionar")

    def test_ambos_admin_o_superadmin(self):
        for fuente in (self.diag, self.fusion):
            self.assertIn('g.permissions.get("superadmin") or g.permissions.get("admin")', fuente)

    def test_mismo_regex_de_normalizacion_del_nudo(self):
        patron = r'\^\(\[A-Za-z\]\*\)0\*\(\\d\+\)\$'
        self.assertRegex(self.diag, patron)
        self.assertRegex(self.fusion, patron)

    def test_mismo_criterio_de_desempate(self):
        criterio = 'key=lambda r: (bool(r["en_manifiesto"]), r["erp_synced_at"] or r["fecha_emision"] or ""),\n            reverse=True,'
        self.assertIn(_norm(criterio), _norm(self.diag))
        self.assertIn(_norm(criterio), _norm(self.fusion))


# ═════════════════════════════════════════════════════════════════════════════
# BLOQUE 12 · Paginación del Monitor (REGLA #4.3, 2026-08-01)
# ═════════════════════════════════════════════════════════════════════════════
class TestCompromisosJsonPaginacion(unittest.TestCase):
    """Daniel 2026-08-01: "hagámosla igual que las etiquetas... contenida en la
    página, no necesitamos darle scroll. Deja eso como regla en el proyecto."
    (REGLA #4.3 de CLAUDE.md). tr_compromisos_json necesita MySQL real, así que
    se prueba ESTRUCTURALMENTE, igual que el resto del archivo."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("tr_compromisos_json")
        cls.norm = _norm(cls.fuente)

    def test_acepta_page_y_per_page_con_parseo_defensivo(self):
        """Mismo patrón try/except que el resto del archivo: un ?page=abc no
        puede tumbar el Monitor con un 500."""
        self.assertIn('request.args.get("page", "1")', self.fuente)
        self.assertIn('request.args.get("per_page", "100")', self.fuente)
        self.assertIn("except (TypeError, ValueError)", self.fuente)

    def test_solo_admite_tamanos_de_pagina_conocidos(self):
        """Clamp defensivo: sin esto, un ?per_page=999999 devuelve todo y la
        paginación deja de proteger nada."""
        self.assertIn("if per_page not in (50, 100, 200, 500):", self.fuente)

    def test_pagina_sobre_result_no_con_limit_offset_en_el_sql(self):
        """LA decisión de diseño de este cambio. El nº de filas que ve el
        operador NO es el nº de filas del SELECT: entre medio se filtra por
        `estado_logistico` (valor derivado en Python) y se expande por ramo
        (PR-B, 1 fila por manifest_item). Un LIMIT/OFFSET en el SQL paginaría
        DOCUMENTOS mientras la grilla muestra FILAS -> páginas de tamaño
        variable y un "Mostrando 1-100 de N" mentiroso."""
        self.assertIn("total_filas   = len(result)", self.fuente)
        self.assertRegex(
            self.norm,
            r"result = result\[_pag_offset:_pag_offset \+ per_page\]",
            "Desapareció el corte de página sobre `result` — si se migró a "
            "LIMIT/OFFSET en el SQL, revisar que el total siga contando FILAS "
            "(post-filtro y post-expansión), no documentos.",
        )

    def test_clampa_la_pagina_fuera_de_rango_en_vez_de_devolver_vacio(self):
        """Si el operador estaba en la página 7 y un filtro deja 2 páginas, se
        muestra la última existente — no una tabla vacía sin explicación."""
        self.assertIn("if page > total_paginas:", self.fuente)

    def test_la_respuesta_trae_lo_que_necesita_el_pie_de_tabla(self):
        """El pie de Etiquetas necesita: total, página actual, tamaño y total de
        páginas. Sin los 4 no se puede pintar "Mostrando A-B de N / Pagina X de Y"."""
        for clave in ('"total": total_filas', '"page": page',
                      '"per_page": per_page', '"total_paginas": total_paginas'):
            self.assertIn(clave, self.fuente, f"Falta {clave} en la respuesta JSON.")

    def test_el_techo_de_500_sigue_ahi_y_documentado(self):
        """LIMITACIÓN CONOCIDA declarada a propósito: el LIMIT del SELECT sigue
        siendo el techo, así que con >500 documentos en un filtro el paginador
        cuenta sobre esos 500. Si alguien sube o quita ese techo, que sea a
        propósito y actualizando este test."""
        self.assertRegex(self.norm, r"LIMIT 500")
        self.assertIn("LIMITACIÓN CONOCIDA", self.fuente,
                      "Se borró la nota de la limitación del techo de 500 — "
                      "esa limitación NO puede quedar sin documentar.")

    def test_los_conteos_no_se_calculan_sobre_la_pagina(self):
        """Los badges cuentan el universo real vía SQL agregado. Si se hubieran
        derivado de `result`, después de paginar contarían solo la página
        visible y los números del Monitor se volverían basura."""
        idx_conteos = self.fuente.index('"conteos": {')
        idx_fin = self.fuente.index("})", idx_conteos)
        self.assertNotIn("result", self.fuente[idx_conteos:idx_fin])
        self.assertNotIn("per_page", self.fuente[idx_conteos:idx_fin])


class TestMonitorFiltrosEnVivo(unittest.TestCase):
    """Bug reportado por Daniel 2026-08-01: "cuando filtras algo y sacas el
    filtro, no se limpia la tabla". CAUSA REAL: cargarMonitor() armaba sus
    parámetros solo desde window.location.search, que únicamente cambia al
    hacer submit del formulario (recarga completa) — todas las recargas AJAX
    del Monitor volvían a pedir el filtro viejo aunque el control en pantalla
    ya estuviera vacío. Agravante: _ramoActual caía a localStorage cuando la
    URL no traía ?clasificacion=, así que "Limpiar filtros" (que navegaba a
    /transporte/ sin query) resucitaba el ramo guardado."""

    @classmethod
    def setUpClass(cls):
        ruta = os.path.join(RAIZ, "static", "transporte_monitor.js")
        with open(ruta, encoding="utf-8") as fh:
            cls.js = fh.read()

    def test_los_filtros_se_leen_del_formulario_vivo_no_solo_de_la_url(self):
        self.assertIn("function _trAplicarFiltrosDelForm(params)", self.js)
        self.assertIn("_trAplicarFiltrosDelForm(params);", self.js,
                      "cargarMonitor() dejó de aplicar los filtros del formulario "
                      "— vuelve el bug de 'saco el filtro y la tabla no cambia'.")

    def test_un_control_vacio_borra_su_parametro(self):
        """El corazón del fix: vacío = params.delete(k), no 'dejar pasar el
        valor viejo de la URL'."""
        self.assertRegex(self.js, r"if \(v\) params\.set\(k, v\);\s*\n\s*else\s+params\.delete\(k\);")

    def test_las_fechas_se_mandan_explicitas_aunque_esten_vacias(self):
        """En el backend "ausente" != "vacío": ausente aplica el default de 30
        días, vacío significa sin filtro de fecha. Si las fechas vacías se
        borraran del querystring, limpiar el filtro de fecha aplicaría un rango
        de 30 días invisible para el operador."""
        self.assertIn("params.set(k, (el.value || '').trim());", self.js)

    def test_limpiar_filtros_tambien_limpia_el_ramo_guardado(self):
        """El agravante: sin esto, "Limpiar filtros" limpia la URL pero
        localStorage.tr_ramo_actual vuelve a aplicar el ramo."""
        self.assertIn("localStorage.removeItem('tr_ramo_actual')", self.js)
        self.assertIn("tr-filters-limpiar", self.js)

    def test_cambiar_cualquier_filtro_vuelve_a_la_pagina_1(self):
        """Si no, el operador que estaba en la página 7 filtra algo con 2
        páginas y ve una tabla vacía sin entender por qué."""
        self.assertIn("function _trResetPagina()", self.js)
        # vista, ramo y el toggle "solo Problema" son los 3 ejes de filtro que
        # recargan por AJAX sin pasar por el formulario.
        self.assertGreaterEqual(self.js.count("_trResetPagina();"), 3)

    def test_el_paginador_manda_page_y_per_page_al_backend(self):
        self.assertIn("params.set('page', String(_pagActual));", self.js)
        self.assertIn("params.set('per_page', String(_perPage));", self.js)


if __name__ == "__main__":
    unittest.main(verbosity=2)

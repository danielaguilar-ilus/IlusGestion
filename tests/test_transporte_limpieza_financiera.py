"""Limpieza del histórico financiero de Transporte (2026-08-05).

QUÉ SE PRUEBA
-------------
`_tr_clasificar_fila_financiera` es la que decide, documento por documento,
si se le corrige el ZZ Envío y si se le borra el costo de courier inventado.
Es plata: con esos dos números el manifiesto calcula el margen y se decide con
qué courier despachar. Un error acá no se nota mirando la pantalla — un margen
inflado se ve igual de bien que uno correcto.

Los dos errores que motivaron el barrido:

  (a) zz_envio guardaba la suma de TODAS las líneas de servicio (envío +
      instalación + retiro), no solo el flete. Factura con envío $30.000 e
      instalación $50.000 mostraba "ZZ Envío $80.000". El error nunca va en
      contra: los servicios suman, así que el despacho SIEMPRE se ve más
      rentable de lo que es.

  (b) costo_courier fue contaminado por un "auto-sane" que copiaba costo_zz
      (lo COBRADO al cliente) como si fuera lo que cobra el transportista, así
      el margen daba siempre $0 y quedaba grabado.

Los cuatro escenarios que pidió Daniel están cubiertos abajo, cada uno con su
test: envío + instalación, solo instalación, envío sin sincronizar, y un
documento ya correcto (que NO debe cambiar).

No usa pytest (no está instalado en este entorno) — unittest estándar, mismo
patrón que el resto de tests/test_*.py: extraer las funciones de app.py vía
AST en vez de importar los ~88K líneas (evita arrastrar Flask/MySQL/ERP).

Correr:  py -m unittest tests.test_transporte_limpieza_financiera -v
"""
import ast
import pathlib
import unittest


APP_PY = pathlib.Path(__file__).resolve().parent.parent / "app.py"

# Las tres funciones puras del barrido + la constante que comparten.
_FUNCIONES = (
    "_tr_cents",
    "_tr_skus_zz_de_fila",
    "_tr_clasificar_fila_financiera",
    "_tr_finanzas_documento_label",
)


_NS_CACHE = None


def cargar_funciones():
    """Extrae las funciones puras de app.py sin importar el módulo entero.

    El resultado se cachea: parsear los ~88K líneas de app.py toma varios
    segundos y hay varias clases de test que necesitan lo mismo.
    """
    global _NS_CACHE
    if _NS_CACHE is not None:
        return _NS_CACHE
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"))
    cuerpo = [n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name in _FUNCIONES]
    faltan = set(_FUNCIONES) - {n.name for n in cuerpo}
    if faltan:
        raise AssertionError(f"No se encontraron en app.py: {sorted(faltan)}")
    ns = {"_ZZ_SKU_ENVIO": "ZZENVIO", "print": lambda *a, **k: None}
    exec(compile(ast.Module(body=cuerpo, type_ignores=[]), "app.py", "exec"), ns)
    _NS_CACHE = ns
    return ns


class BaseFinanzas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = cargar_funciones()
        cls.clasificar = staticmethod(ns["_tr_clasificar_fila_financiera"])
        cls.cents = staticmethod(ns["_tr_cents"])
        cls.skus = staticmethod(ns["_tr_skus_zz_de_fila"])
        cls.label = staticmethod(ns["_tr_finanzas_documento_label"])

    def fila(self, **kw):
        """Fila de transport_commitments con los defaults de un documento sano."""
        base = {
            "id": 1, "tido": "FCV", "nudo": "0000011149",
            "zz_envio": None, "costo_zz": 0, "costo_courier": None,
            "costo_envio": 0, "finanzas_revisado_at": None,
            "zz_skus": "", "skus_lineas": "",
            "peso_predominante": 0, "tiene_productos_json": 0,
        }
        base.update(kw)
        return base


# ─────────────────────────────────────────────────────────────────────────────
# (a) ZZ ENVÍO — los cuatro escenarios que pidió Daniel
# ─────────────────────────────────────────────────────────────────────────────
class ZzEnvioTest(BaseFinanzas):

    def test_envio_mas_instalacion_se_corrige_con_el_dato_local(self):
        """EL CASO DE DANIEL: envío $30.000 + instalación $50.000 → guardó $80.000.

        El monto correcto NO hay que ir a buscarlo al ERP: ya está en
        `costo_envio`, la columna que el sync masivo llena sumando solo las
        líneas ZZENVIO. Corregir con el dato local es exacto, instantáneo y
        —lo más importante— CONVERGE: a la segunda corrida ya coinciden.
        """
        d = self.clasificar(self.fila(
            zz_envio=80000, costo_zz=80000, costo_envio=30000,
            zz_skus="ZZENVIO,ZZINSTALACION"))
        self.assertEqual(d["zz_accion"], "corregir_local")
        self.assertEqual(d["zz_nuevo"], 30000)
        self.assertIn("ZZINSTALACION", d["zz_motivo"])

    def test_en_el_documento_mezclado_el_cobrado_baja(self):
        """En una factura con envío + instalación, el monto guardado traía los
        dos sumados: corregirlo BAJA el cobrado y con eso baja el margen que se
        venía mostrando. El despacho se veía más rentable de lo que era.

        (Ojo: esto vale para el documento mezclado, que es el caso del bug. No
        es una propiedad universal de la corrección — un documento con una sola
        línea de envío puede quedar igual, y si su cantidad no es 1 el valor
        neto de la línea puede incluso ser mayor que el precio unitario que se
        guardaba antes.)
        """
        d = self.clasificar(self.fila(
            zz_envio=80000, costo_envio=30000, zz_skus="ZZENVIO,ZZINSTALACION"))
        self.assertLess(d["zz_nuevo"], 80000)

    def test_solo_instalacion_queda_sin_cobro_no_en_cero(self):
        """Documento que NO cobra flete: se anula, y el motivo lo explica.

        Se deja en NULL (= "no cobra flete"), nunca en 0. Un 0 se lee como
        "se cobró cero" y el manifiesto lo pinta como pérdida total sobre un
        despacho de garantía o cortesía que jamás tuvo cobro.
        """
        d = self.clasificar(self.fila(
            zz_envio=50000, costo_zz=50000, zz_skus="ZZINSTALACION"))
        self.assertEqual(d["zz_accion"], "anular")
        self.assertIn("no tiene línea de envío", d["zz_motivo"])

    def test_solo_instalacion_ya_anulado_no_se_vuelve_a_tocar(self):
        """Idempotencia: correr el barrido dos veces no hace nada la segunda."""
        d = self.clasificar(self.fila(zz_envio=None, zz_skus="ZZINSTALACION"))
        self.assertEqual(d["zz_accion"], "sin_cambio")

    def test_envio_sin_sincronizar_se_resincroniza(self):
        """Cobra flete pero el sync masivo nunca pasó (zz_skus vacío).

        No se puede caer a costo_zz para rellenarlo: esa columna la sobrescribe
        el flujo de manifiesto con el costo del courier, así que copiarla sería
        volver a inventar el número. Este es el ÚNICO caso que va al ERP.
        """
        for valor in (None, 0):
            with self.subTest(zz_envio=valor):
                d = self.clasificar(self.fila(
                    zz_envio=valor, costo_zz=45000,
                    zz_skus="", skus_lineas="ZZENVIO"))
                self.assertEqual(d["zz_accion"], "resincronizar")

    def test_documento_ya_correcto_no_cambia(self):
        """Solo tiene línea de envío y el monto coincide con el flete real."""
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=30000, costo_envio=30000,
            zz_skus="ZZENVIO", costo_courier=21500))
        self.assertEqual(d["zz_accion"], "sin_cambio")
        self.assertEqual(d["costo_accion"], "sin_cambio")

    def test_varias_lineas_de_envio_siguen_siendo_solo_flete(self):
        """Dos ZZENVIO y nada más: la suma sigue siendo 100% flete."""
        d = self.clasificar(self.fila(
            zz_envio=60000, costo_envio=60000,
            zz_skus="ZZENVIO", skus_lineas="ZZENVIO,ZZENVIO"))
        self.assertEqual(d["zz_accion"], "sin_cambio")

    def test_sin_lineas_zz_no_se_toca(self):
        """Sin saber si cobra flete, no se toca: el lado seguro es no escribir."""
        d = self.clasificar(self.fila(zz_envio=12345))
        self.assertEqual(d["zz_accion"], "sin_clasificar")


class ConvergenciaTest(BaseFinanzas):
    """EL BARRIDO TIENE QUE TERMINAR.

    La pantalla le dice a Daniel "vuelve a apretar hasta que no queden". Si una
    clase de documento se vuelve a marcar corrida tras corrida, ese mensaje es
    un bucle infinito: se releerían siempre los mismos y nunca se llegaría al
    resto. Cada acción tiene que dejar la fila en un estado que la próxima
    corrida clasifique como "sin_cambio".
    """

    def _tras_corregir(self, fila, d):
        """Simula la fila DESPUÉS de aplicar la corrección."""
        nueva = dict(fila)
        if d["zz_accion"] == "corregir_local":
            nueva["zz_envio"] = d["zz_nuevo"]
        elif d["zz_accion"] == "anular":
            nueva["zz_envio"] = None
        elif d["zz_accion"] == "resincronizar":
            # _tr_fetch_from_erp repuebla el monto y el barrido deja la marca.
            nueva["finanzas_revisado_at"] = "2026-08-05 19:00:00"
        if d["costo_accion"] == "limpiar":
            nueva["costo_courier"] = None
        return nueva

    def _converge(self, fila):
        d1 = self.clasificar(fila)
        d2 = self.clasificar(self._tras_corregir(fila, d1))
        self.assertEqual(d2["zz_accion"], "sin_cambio",
                         f"zz sigue pendiente tras corregir (era {d1['zz_accion']})")
        self.assertEqual(d2["costo_accion"], "sin_cambio",
                         f"costo sigue pendiente tras corregir (era {d1['costo_accion']})")

    def test_converge_envio_mas_instalacion(self):
        self._converge(self.fila(
            zz_envio=80000, costo_zz=80000, costo_envio=30000,
            zz_skus="ZZENVIO,ZZINSTALACION"))

    def test_converge_solo_instalacion(self):
        self._converge(self.fila(
            zz_envio=50000, costo_zz=50000, zz_skus="ZZINSTALACION"))

    def test_converge_envio_sin_sincronizar(self):
        """Este es el que NO convergía: su clasificación depende de los SKUs,
        que no cambian al releer el documento. La marca finanzas_revisado_at
        es lo que lo saca de la cola."""
        self._converge(self.fila(
            zz_envio=0, costo_zz=45000, zz_skus="", skus_lineas="ZZENVIO"))

    def test_converge_flete_real_de_cero(self):
        """Documento cuyo flete en el ERP es de verdad $0: releerlo deja el
        mismo 0, así que sin la marca volvería a la cola para siempre."""
        self._converge(self.fila(
            zz_envio=0, zz_skus="", skus_lineas="ZZENVIO"))

    def test_converge_fila_vieja_sin_costo_envio(self):
        """Fila anterior al 2026-06-13: se pregunta al ERP UNA vez, y la marca
        evita que vuelva a la cola en cada corrida."""
        self._converge(self.fila(
            zz_envio=30000, costo_envio=0, zz_skus="ZZENVIO"))

    def test_converge_costo_inventado(self):
        self._converge(self.fila(
            zz_envio=30000, costo_zz=30000, costo_envio=30000,
            costo_courier=30000, zz_skus="ZZENVIO"))

    def test_una_sola_pasada_alcanza_en_todas_las_combinaciones(self):
        """BARRIDO EXHAUSTIVO: ninguna combinación queda pendiente ni escribe $0.

        Recorre todas las combinaciones de los campos que mira el clasificador
        y verifica tres propiedades sobre cada una:

          1. Tras aplicar la corrección, la segunda pasada dice "sin_cambio".
             Si algo se re-marcara para siempre, el mensaje "vuelve a apretar
             hasta que no queden" sería un bucle infinito.
          2. Un costo inventado se detecta en la PRIMERA pasada. Antes hacía
             falta una segunda (después de corregir zz_envio), así que la
             primera le decía a Daniel que estaba todo listo mientras el
             número falso seguía en la base.
          3. Ninguna corrección local escribe 0 en zz_envio: un 0 se lee como
             "se cobró cero" y pinta pérdida total sobre un despacho real.
        """
        import itertools
        combinaciones = itertools.product(
            [None, 0, 30000, 80000],            # zz_envio
            [0, 30000, 50000],                  # costo_envio
            ["", "ZZENVIO", "ZZINSTALACION", "ZZENVIO,ZZINSTALACION"],
            ["", "ZZENVIO", "ZZINSTALACION"],   # skus_lineas
            [0, 30000, 80000],                  # costo_zz
            [None, 0, 30000, 80000],            # costo_courier
            [0, 1],                             # cubicaje declarado
        )
        n = 0
        for zz, ev, sk, li, cz, cc, cu in combinaciones:
            n += 1
            f = self.fila(zz_envio=zz, costo_envio=ev, zz_skus=sk, skus_lineas=li,
                          costo_zz=cz, costo_courier=cc, tiene_productos_json=cu)
            d1 = self.clasificar(f)
            if d1["zz_accion"] == "corregir_local":
                self.assertTrue(d1["zz_nuevo"],
                                f"corrección a $0 con {f}")
            d2 = self.clasificar(self._tras_corregir(f, d1))
            # "sin_clasificar" también es terminal: el documento no tiene
            # ninguna línea de servicio registrada, así que no hay nada que
            # corregir y el barrido no lo escribe nunca.
            self.assertIn(d2["zz_accion"], ("sin_cambio", "sin_clasificar"),
                          f"zz no converge: {d1['zz_accion']} → {d2['zz_accion']} con {f}")
            self.assertNotEqual(d2["costo_accion"], "limpiar",
                                f"el costo falso recién se detecta en la 2ª pasada con {f}")
        self.assertGreater(n, 3000)

    def test_una_vez_marcado_no_se_relee_mas(self):
        d = self.clasificar(self.fila(
            zz_envio=0, zz_skus="", skus_lineas="ZZENVIO",
            finanzas_revisado_at="2026-08-05 19:00:00"))
        self.assertEqual(d["zz_accion"], "sin_cambio")

    def test_la_marca_no_frena_la_correccion_local(self):
        """La marca es solo para la relectura del ERP. Un documento con dato
        local malo se corrige igual, esté marcado o no."""
        d = self.clasificar(self.fila(
            zz_envio=80000, costo_envio=30000, zz_skus="ZZENVIO,ZZINSTALACION",
            finanzas_revisado_at="2026-08-05 19:00:00"))
        self.assertEqual(d["zz_accion"], "corregir_local")


class SkusDeDosFuentesTest(BaseFinanzas):
    """Las dos fuentes de líneas ZZ se UNEN; ninguna es completa por sí sola.

    Un documento que entró solo por el cubicador tiene sus líneas en
    transport_commitment_lines y `zz_skus` vacío. Si se clasificara mirando
    solo `zz_skus`, parecería "sin líneas ZZ"; peor, si se mirara solo esa
    columna para decidir "no cobra flete", se borraría un cobro real.
    """

    def test_solo_zz_skus(self):
        self.assertEqual(self.skus({"zz_skus": "ZZENVIO,ZZRETIRO"}),
                         {"ZZENVIO", "ZZRETIRO"})

    def test_solo_lineas(self):
        self.assertEqual(self.skus({"skus_lineas": "ZZINSTALACION"}),
                         {"ZZINSTALACION"})

    def test_se_unen_y_normalizan(self):
        self.assertEqual(
            self.skus({"zz_skus": " zzenvio , ", "skus_lineas": "ZZENVIO,ZZRETIRO"}),
            {"ZZENVIO", "ZZRETIRO"})

    def test_lineas_salvan_documento_sin_zz_skus(self):
        """Con zz_skus vacío pero líneas de instalación, NO se clasifica como
        'sin flete' por error: se ve la instalación y se anula bien."""
        d = self.clasificar(self.fila(
            zz_envio=50000, zz_skus="", skus_lineas="ZZINSTALACION"))
        self.assertEqual(d["zz_accion"], "anular")

    def test_ignora_skus_que_no_son_de_servicio(self):
        self.assertEqual(self.skus({"zz_skus": "ZZENVIO,BICI-01"}), {"ZZENVIO"})

    def test_si_las_fuentes_se_contradicen_no_se_anula(self):
        """El sync masivo no vio línea de envío, pero el individual SÍ la
        registró. Mirando solo el masivo se anularía un cobro que existe.
        Ante la contradicción se le pregunta al ERP, que es el único que
        puede desempatar."""
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_envio=0,
            zz_skus="ZZINSTALACION", skus_lineas="ZZENVIO,ZZINSTALACION"))
        self.assertNotEqual(d["zz_accion"], "anular")
        self.assertEqual(d["zz_accion"], "resincronizar")


# ─────────────────────────────────────────────────────────────────────────────
# (b) COSTO COURIER INVENTADO
# ─────────────────────────────────────────────────────────────────────────────
class CostoCourierTest(BaseFinanzas):

    def test_costo_igual_a_lo_cobrado_se_limpia(self):
        """La firma del auto-sane: costo == costo_zz == zz_envio → margen $0."""
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=30000, costo_courier=30000,
            zz_skus="ZZENVIO"))
        self.assertEqual(d["costo_accion"], "limpiar")

    def test_costo_cotizado_de_verdad_no_se_toca(self):
        """El cubicador dejó costo_zz == costo_courier == precio COTIZADO.

        Coincide con costo_zz igual que una fila contaminada, pero NO coincide
        con zz_envio (lo cobrado al cliente): es una cotización real y borrarla
        sería perder un dato que costó conseguir.
        """
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=21500, costo_courier=21500,
            zz_skus="ZZENVIO", peso_predominante=42.5))
        self.assertEqual(d["costo_accion"], "sin_cambio")

    def test_sin_costo_registrado_no_se_toca(self):
        for valor in (None, 0):
            with self.subTest(costo_courier=valor):
                d = self.clasificar(self.fila(
                    costo_courier=valor, zz_envio=30000, zz_skus="ZZENVIO"))
                self.assertEqual(d["costo_accion"], "sin_cambio")

    def test_dudoso_cuando_no_hay_zz_envio_para_confirmar(self):
        """Coincide con costo_zz y nunca pasó por el cubicador, pero sin
        zz_envio no hay prueba. Se reporta, NO se limpia solo."""
        d = self.clasificar(self.fila(
            zz_envio=None, costo_zz=30000, costo_courier=30000,
            zz_skus="ZZENVIO"))
        self.assertEqual(d["costo_accion"], "dudoso")

    def test_despacho_a_costo_no_se_borra(self):
        """Los TRES montos coinciden, pero es una cotización REAL.

        En un despacho a costo se le traspasa al cliente exactamente lo que
        cobra el courier, así que cobrado == costo. Eso calza con la firma del
        auto-sane. Lo que lo salva es el cubicaje declarado: el cubicador
        escribe peso y productos junto con el costo, y el proceso que inventaba
        el margen no escribía nada más que el costo.
        """
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=30000, costo_envio=30000, costo_courier=30000,
            zz_skus="ZZENVIO", peso_predominante=42.5))
        self.assertEqual(d["costo_accion"], "sin_cambio")

    def test_con_cubicaje_declarado_no_es_dudoso(self):
        """Si trae cubicaje, pasó por el cubicador: el costo es suyo, no del
        auto-sane (que solo escribía costo_courier y nada más)."""
        for campo, valor in (("peso_predominante", 42.5), ("tiene_productos_json", 1)):
            with self.subTest(campo=campo):
                d = self.clasificar(self.fila(**{
                    "zz_envio": None, "costo_zz": 30000, "costo_courier": 30000,
                    "zz_skus": "ZZENVIO", campo: valor}))
                self.assertEqual(d["costo_accion"], "sin_cambio")

    def test_idempotente_tras_limpiar(self):
        """Segunda corrida: ya está en NULL, no hay nada que limpiar."""
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=30000, costo_courier=None, zz_skus="ZZENVIO"))
        self.assertEqual(d["costo_accion"], "sin_cambio")


class ComparacionDeMontosTest(BaseFinanzas):
    """La detección de (b) es una igualdad exacta de montos: no puede fallar
    por representación binaria. Los montos llegan de MySQL como Decimal."""

    def test_cents_redondea_a_dos_decimales(self):
        self.assertEqual(self.cents(30000), 3000000)
        self.assertEqual(self.cents("30000.00"), 3000000)
        self.assertIsNone(self.cents(None))
        self.assertIsNone(self.cents("no es un monto"))

    def test_decimal_y_float_se_comparan_igual(self):
        from decimal import Decimal
        d = self.clasificar(self.fila(
            zz_envio=Decimal("30000.00"), costo_zz=Decimal("30000.00"),
            costo_courier=30000.0, zz_skus="ZZENVIO"))
        self.assertEqual(d["costo_accion"], "limpiar")

    def test_diferencia_de_un_peso_no_es_contaminacion(self):
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=30000, costo_courier=29999,
            zz_skus="ZZENVIO", peso_predominante=10))
        self.assertEqual(d["costo_accion"], "sin_cambio")


class EscrituraDelFleteTest(unittest.TestCase):
    """Guardas sobre `_tr_fetch_from_erp`, el otro escritor de `zz_envio`.

    No se puede ejecutar acá (habla con el ERP y con MySQL), así que se
    comprueban sobre el código fuente las dos propiedades de las que depende
    que el barrido no se pelee con él.
    """

    @classmethod
    def setUpClass(cls):
        src = APP_PY.read_text(encoding="utf-8")
        arbol = ast.parse(src)
        cls.src = next(
            ast.get_source_segment(src, n) for n in arbol.body
            if isinstance(n, ast.FunctionDef) and n.name == "_tr_fetch_from_erp")

    def test_usa_el_valor_de_la_linea_y_no_el_precio_unitario(self):
        """Tiene que sumar VANELI (valor neto de la línea), igual que el sync
        masivo cuando llena `costo_envio`.

        Con fórmulas distintas los dos escritores se pisan: el cron horario
        deja el precio unitario, el barrido deja el valor de la línea, y el
        mismo documento oscila entre dos montos para siempre. El barrido lo
        volvería a marcar en cada corrida y nunca terminaría.
        """
        self.assertIn("VANELI", self.src,
                      "volvió a sumar solo PPPRNE: el monto del flete deja de "
                      "coincidir con costo_envio y el barrido no converge")
        self.assertIn("_valor_linea", self.src)

    def test_una_lectura_vacia_no_borra_el_cobro(self):
        """Un documento sin NINGUNA línea es una lectura fallida, no un
        documento sin flete: la consulta al ERP se traga sus errores y devuelve
        el encabezado con las líneas vacías. Sin la guarda, un timeout pasajero
        dejaba en NULL el flete de una factura buena."""
        self.assertIn("if raw_lineas:", self.src)


class EtiquetaDocumentoTest(BaseFinanzas):
    def test_quita_el_relleno_de_ceros(self):
        self.assertEqual(
            self.label({"id": 3, "tido": "FCV", "nudo": "0000011149"}),
            "FCV 11149")

    def test_cae_al_id_si_no_hay_documento(self):
        self.assertEqual(self.label({"id": 7, "tido": "", "nudo": ""}), "#7")


class CombinacionesTest(BaseFinanzas):
    """Las dos correcciones son independientes: una fila puede necesitar las
    dos, una sola, o ninguna."""

    def test_instalacion_pura_con_costo_inventado(self):
        d = self.clasificar(self.fila(
            zz_envio=50000, costo_zz=50000, costo_courier=50000,
            zz_skus="ZZINSTALACION"))
        self.assertEqual(d["zz_accion"], "anular")
        self.assertEqual(d["costo_accion"], "limpiar")

    def test_documento_sano_no_dispara_ninguna_correccion(self):
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_zz=21500, costo_envio=30000, costo_courier=21500,
            zz_skus="ZZENVIO", peso_predominante=42.5, tiene_productos_json=1))
        self.assertEqual(d["zz_accion"], "sin_cambio")
        self.assertEqual(d["costo_accion"], "sin_cambio")


class NoBorrarUnCobroRealTest(BaseFinanzas):
    """`costo_envio` en 0 NO significa "el flete fue $0".

    La columna se creó el 2026-06-13 y el ALTER rellenó con 0 todas las filas
    que ya existían. Una factura anterior a esa fecha, con $30.000 de flete
    legítimo guardados en zz_envio, tiene costo_envio=0 sin que eso signifique
    nada. Copiar ese 0 encima BORRARÍA el cobro y el manifiesto lo mostraría
    como pérdida total.

    Este test existe porque la primera versión del barrido hacía exactamente
    eso; lo agarró la suite antes de tocar producción.
    """

    def test_no_pisa_un_cobro_real_con_un_costo_envio_en_cero(self):
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_envio=0, zz_skus="ZZENVIO"))
        self.assertNotEqual(d["zz_accion"], "corregir_local")
        self.assertIsNone(d["zz_nuevo"])

    def test_ante_la_duda_se_le_pregunta_al_erp(self):
        d = self.clasificar(self.fila(
            zz_envio=30000, costo_envio=0, zz_skus="ZZENVIO"))
        self.assertEqual(d["zz_accion"], "resincronizar")

    def test_ninguna_correccion_local_deja_el_flete_en_cero(self):
        """Regla transversal: el barrido nunca escribe 0 en zz_envio. O corrige
        con un monto real, o lo deja en NULL ("no cobra flete"), o pregunta."""
        casos = [
            self.fila(zz_envio=30000, costo_envio=0, zz_skus="ZZENVIO"),
            self.fila(zz_envio=80000, costo_envio=0, zz_skus="ZZENVIO,ZZINSTALACION"),
            self.fila(zz_envio=45000, costo_envio=0, zz_skus="ZZENVIO,ZZRETIRO"),
        ]
        for fila in casos:
            with self.subTest(zz_skus=fila["zz_skus"]):
                d = self.clasificar(fila)
                if d["zz_accion"] == "corregir_local":
                    self.assertGreater(d["zz_nuevo"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

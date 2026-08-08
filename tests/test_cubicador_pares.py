"""Pruebas del cálculo de peso/volumen para SKUs que se venden de a PAR.

BUG REAL (2026-08-07, Daniel, caso FCV 11225 — "estamos inflando los precios,
el peso se está duplicando... eso se tiene que reparar sí o sí"):

Los discos y mancuernas se empacan de a PAR y así están cargados en el
Catálogo (la ficha del "Par Discos Training Bumpers ILUS 5 kg" pesa 10,5 kg =
2 discos de 5 kg + empaque). Pero el ERP factura en PIEZAS SUELTAS: la
FCV 11225 trae CANT=4 para ese SKU, que son 4 discos = 2 pares.

El cubicador hacía `peso_de_la_ficha x cantidad_del_ERP`:
    10,5 kg x 4 = 42,0 kg     ← lo que salía en la cotización
cuando el envío real pesa:
    10,5 kg x (4/2) = 21,0 kg ← 2 pares

O sea, el DOBLE, en toda cotización de flete de discos y mancuernas desde
que existe el módulo. El reporte real de la FCV 11225 daba 470,5 kg de peso
predominante total cuando el correcto es 260,5 kg.

La corrección NO toca la cantidad del ERP: bodega sigue viendo 4 piezas para
el picking y las cuentas siguen cuadrando contra la factura. Solo el cálculo
de peso/volumen divide por `unidades_por_venta` (columna nueva en el
Catálogo, default 1 = sin cambio para máquinas y accesorios).

Se replica EXACTAMENTE la aritmética de _cubicador_fetch (app.py) en vez de
extraerla con ast: esa función tiene ~300 líneas atadas a red (ERP REST/SQL)
y BD, imposible de ejecutar aislada. Lo que se prueba acá es la REGLA de
negocio con los números reales de la factura de Daniel; el test de
caracterización del cubicador cubre el resto de la función.

Correr con:  py -m unittest tests.test_cubicador_pares -v
"""
import re
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()


def totales_linea(peso_ficha_kg, peso_vol_ficha, vol_ficha_cm3, cantidad_erp,
                  unidades_por_venta=1, caprco2=0, rludpr=0):
    """Misma aritmética que _cubicador_fetch.

    Fuente de la conversión, en orden:
      1. RLUDPR  — razón de transformación del ERP (piezas por unidad 2)
      2. unidades_por_venta — respaldo manual del Catálogo
    Y para la cantidad de empaques se prefiere CAPRCO2 (el dato que el propio
    ERP ya calculó) sobre dividir a mano.
    """
    uxv = 1
    if (rludpr or 0) > 1:
        uxv = float(rludpr)
    elif max(int(unidades_por_venta or 1), 1) > 1:
        uxv = max(int(unidades_por_venta or 1), 1)

    if uxv > 1:
        equiv = caprco2 if (caprco2 or 0) > 0 else (cantidad_erp / uxv)
    else:
        equiv = cantidad_erp

    pred_u = max(peso_ficha_kg, peso_vol_ficha)
    return {
        "peso_kg_tot":  round(peso_ficha_kg * equiv, 4),
        "peso_vol_tot": round(peso_vol_ficha * equiv, 4),
        "vol_tot":      round(vol_ficha_cm3 * equiv, 2),
        "pred_tot":     round(pred_u * equiv, 4),
    }


class TestFacturaRealFCV11225(unittest.TestCase):
    """Los 4 SKUs de disco de la factura que Daniel fotografió, con los
    pesos exactos que salían mal en el reporte impreso."""

    # (descripcion, peso de la ficha = peso del PAR, cantidad ERP, total que
    #  salía ANTES (mal), total correcto)
    CASOS = [
        ("Par Discos Training Bumpers ILUS 5 kg",  10.5, 4, 42.0,  21.0),
        ("Par Discos Training Bumpers ILUS 10 kg", 20.5, 4, 82.0,  41.0),
        ("Par Discos Training Bumpers ILUS 15 kg", 32.0, 4, 128.0, 64.0),
        ("Par Discos Training Bumpers ILUS 20 kg", 42.0, 4, 168.0, 84.0),
    ]

    def test_cada_disco_pesa_la_mitad_de_lo_que_decia(self):
        for desc, peso_par, cant, total_malo, total_bueno in self.CASOS:
            with self.subTest(desc):
                antes = totales_linea(peso_par, 0, 0, cant)
                self.assertEqual(total_malo, antes["peso_kg_tot"],
                                 "el test no reproduce el bug original")
                # Como lo entrega Random: CAPRCO1=4 piezas, CAPRCO2=2 pares,
                # RLUDPR=2 (razón de transformación).
                ahora = totales_linea(peso_par, 0, 0, cant, caprco2=cant / 2, rludpr=2)
                self.assertEqual(total_bueno, ahora["peso_kg_tot"])

    def test_cuatro_piezas_son_dos_pares(self):
        # La regla de negocio en una línea: 4 discos facturados = 2 empaques.
        ahora = totales_linea(10.5, 0, 0, 4, caprco2=2, rludpr=2)
        self.assertEqual(21.0, ahora["peso_kg_tot"])
        self.assertEqual(2 * 10.5, ahora["peso_kg_tot"])

    def test_se_prefiere_la_cantidad_que_ya_calculo_el_erp(self):
        """CAPRCO2 manda sobre dividir a mano: si el ERP resolvió una venta
        mixta o un redondeo, ese número es la verdad, no nuestra división."""
        r = totales_linea(10.5, 0, 0, 5, caprco2=2, rludpr=2)
        self.assertEqual(21.0, r["peso_kg_tot"], "debió usar CAPRCO2=2, no 5/2")

    def test_sin_caprco2_cae_a_la_razon_de_transformacion(self):
        # Documento viejo o línea sin unidad 2 cargada: se divide por RLUDPR.
        r = totales_linea(10.5, 0, 0, 4, caprco2=0, rludpr=2)
        self.assertEqual(21.0, r["peso_kg_tot"])

    def test_peso_predominante_total_del_documento(self):
        """El número grande del reporte: 470,5 kg -> 260,5 kg.

        Incluye el Porta Bumpers (SKU 1120100292), que NO es par: CANT=1 y
        su predominante es el peso volumétrico (50,5 pv > 18,0 kg). Ese no
        debe cambiar — es la prueba de que el fix no toca lo que ya estaba
        bien."""
        porta = totales_linea(18.0, 50.5, 202075.0, 1)
        self.assertEqual(50.5, porta["pred_tot"], "el Porta Bumpers no debe cambiar")

        discos_antes = sum(
            totales_linea(peso, 0, 0, cant)["pred_tot"]
            for _, peso, cant, _, _ in self.CASOS)
        discos_ahora = sum(
            totales_linea(peso, 0, 0, cant, caprco2=cant / 2, rludpr=2)["pred_tot"]
            for _, peso, cant, _, _ in self.CASOS)

        self.assertEqual(470.5, round(porta["pred_tot"] + discos_antes, 1))
        self.assertEqual(260.5, round(porta["pred_tot"] + discos_ahora, 1))


class TestNoRompeLoQueYaFuncionaba(unittest.TestCase):
    """unidades_por_venta=1 (el default, o sea TODO el catálogo salvo los
    SKUs que Daniel marque) tiene que dar EXACTAMENTE el mismo resultado que
    antes del cambio."""

    def test_maquina_suelta_no_cambia(self):
        # Producto sin unidad 2 en el ERP: RLUDPR llega en 0 o 1.
        for razon in (0, 1):
            with self.subTest(rludpr=razon):
                r = totales_linea(125.0, 0, 0, 3, rludpr=razon)
                self.assertEqual(375.0, r["peso_kg_tot"])

    def test_default_ausente_se_trata_como_uno(self):
        for valor_raro in (None, 0, "", "1"):
            with self.subTest(valor_raro):
                r = totales_linea(125.0, 0, 0, 3, unidades_por_venta=valor_raro or 1)
                self.assertEqual(375.0, r["peso_kg_tot"])

    def test_respaldo_manual_si_el_erp_no_tiene_unidad_2(self):
        # Producto que de verdad se empaca de a par pero al que nadie le
        # configuró la unidad 2 en Random: la marca del Catálogo lo salva.
        r = totales_linea(10.5, 0, 0, 4, unidades_por_venta=2, rludpr=0)
        self.assertEqual(21.0, r["peso_kg_tot"])

    def test_el_erp_manda_sobre_la_marca_manual(self):
        # Si ambos existen, gana Random: es la fuente de verdad de la empresa.
        r = totales_linea(10.5, 0, 0, 4, unidades_por_venta=2, caprco2=2, rludpr=2)
        self.assertEqual(21.0, r["peso_kg_tot"])

    def test_volumen_y_peso_volumetrico_tambien_se_corrigen(self):
        # El flete se cobra por el MAYOR entre peso real y volumétrico: si
        # solo se corrigiera el peso real, un producto voluminoso seguiría
        # cotizando al doble.
        r = totales_linea(10.5, 8.0, 12422.0, 4, caprco2=2, rludpr=2)
        self.assertEqual(21.0, r["peso_kg_tot"])
        self.assertEqual(16.0, r["peso_vol_tot"])
        self.assertEqual(24844.0, r["vol_tot"])

    def test_cantidad_impar_de_piezas_no_se_pierde(self):
        # 3 discos sueltos y el ERP sin CAPRCO2 = 1,5 pares. Se cobra 1,5 x el
        # peso del par, NO se trunca a 1 (perder medio par sería cotizar de menos).
        r = totales_linea(10.5, 0, 0, 3, rludpr=2)
        self.assertEqual(15.75, r["peso_kg_tot"])


class TestElCodigoDeAppPyUsaLaRegla(unittest.TestCase):
    """Verifica contra el app.py REAL que los 4 totales pasan por
    bultos_equivalentes. Si alguien vuelve a poner `* qty` en cualquiera de
    ellos, el bug regresa en silencio (el peso se ve "razonable" igual)."""

    def test_los_cuatro_totales_usan_bultos_equivalentes(self):
        for campo in ("peso_kg_tot", "peso_vol_tot", "vol_tot", "pred_tot"):
            with self.subTest(campo):
                patron = rf'"{campo}":\s*round\([a-z_]+\s*\*\s*bultos_equivalentes'
                self.assertRegex(
                    APP_SRC, patron,
                    f"{campo} dejó de usar bultos_equivalentes -- el flete de "
                    f"discos y mancuernas vuelve a cotizarse al doble")

    def test_la_query_al_erp_trae_la_unidad_2(self):
        """La corrección de fondo (Daniel: "muda esa columna"): sin estos
        campos en el SELECT a MAEDDO, el sistema no tiene forma de saber que
        4 discos son 2 pares y todo lo demás es adivinanza."""
        for campo in ("CAPRCO2", "RLUDPR", "UD02PR"):
            with self.subTest(campo):
                self.assertIn(campo, APP_SRC,
                              f"{campo} salió del SELECT a MAEDDO -- el peso "
                              f"vuelve a calcularse solo con la unidad 1")

    def test_la_columna_se_garantiza_en_boot(self):
        # Con ILUS_SKIP_MIGRATIONS=1 en producción, sin este _ensure_ la
        # columna no existe y la query del cubicador revienta.
        self.assertIn("def _ensure_producto_unidades_por_venta", APP_SRC)
        self.assertIn("_ensure_producto_unidades_por_venta()", APP_SRC)

    def test_el_formulario_del_catalogo_guarda_el_campo(self):
        self.assertIn("unidades_por_venta=%s", APP_SRC)
        with open("templates/product_form.html", encoding="utf-8", errors="ignore") as fh:
            html = fh.read()
        self.assertIn('name="unidades_por_venta"', html)

    def test_el_reporte_avisa_cuando_hay_productos_de_a_par(self):
        # Pedido explícito de Daniel: "recuerda indicar que algunos productos
        # se verán como pares, como mancuernas, discos y algunos accesorios".
        self.assertIn("Productos con unidad secundaria", APP_SRC)
        self.assertIn("{_nota_pares}", APP_SRC)


class TestTodosLosModulosAplicanLaMismaRegla(unittest.TestCase):
    """Daniel pidió "un cambio general... tanto en cubicación como en todo".

    El peso se calcula en CUATRO archivos distintos. Si uno solo sigue
    multiplicando por las piezas facturadas, ese módulo vuelve a cotizar al
    doble y el sistema queda incoherente consigo mismo (el Cubicador diría
    21 kg y la cotización de flete 42 kg para el mismo pedido).
    """

    def _leer(self, ruta):
        with open(ruta, encoding="utf-8", errors="ignore") as fh:
            return fh.read()

    def test_cotizador_de_flete_divide_por_empaque(self):
        src = self._leer("logistica_cotizaciones.py")
        self.assertIn("unidades_por_venta", src,
                      "el cotizador de flete no conoce los productos de a par")
        self.assertNotIn('peso_kg = round(b["peso_kg_u"] * cantidad, 3)', src,
                         "el cotizador volvió a multiplicar por las piezas facturadas")

    def test_modal_de_medidas_divide_por_empaque(self):
        src = self._leer("cubicador_plus.py")
        self.assertIn("unidades_por_venta", src)
        self.assertNotIn('"peso_kg_tot":  _r(peso_kg * qty, 4)', src,
                         "el modal de medidas volvió a multiplicar por piezas")

    def test_retiros_divide_por_empaque(self):
        src = self._leer("pickups_module.py")
        self.assertIn("def _empaques_equivalentes", src)
        self.assertNotIn("peso_unit * qty_sel,", src,
                         "quedó un guardado de retiro multiplicando por piezas")

    def test_las_pantallas_no_recalculan_por_su_cuenta(self):
        # El navegador recalculaba el total con peso_u * cantidad, saltándose
        # la corrección del backend aunque la fila mostrara el valor correcto.
        asignar = self._leer("static/cubicador_asignar.js")
        self.assertNotIn("(l.peso_kg_u*l.cantidad)", asignar)
        self.assertNotIn("(parseFloat(l.peso_kg_u)  || 0) * qty", asignar)
        tabs = self._leer("static/cubicador_tabs.js")
        self.assertIn("bultos_equivalentes", tabs)
        self.assertNotIn("l.peso_kg_tot = Math.round(t.kg * qty * 10000) / 10000", tabs)

    def test_al_guardar_medidas_los_totales_van_por_empaque(self):
        # Guardar medidas desde el Cubicador reescribe los data-* de la fila.
        # Si eso vuelve a usar `qty`, el peso queda al doble en pantalla
        # aunque el backend lo haya calculado bien.
        tabs = self._leer("static/cubicador_tabs.js")
        for patron in ("t.kg * qty", "t.pv * qty", "t.vol * qty", "t.pred * qty"):
            with self.subTest(patron):
                self.assertNotIn(patron, tabs,
                                 f"'{patron}' volvió: al guardar medidas el peso "
                                 f"de discos/mancuernas se muestra al doble")
        self.assertIn("data-uxv", self._leer("templates/cubicador/index.html"),
                      "el template dejó de emitir data-uxv y el JS no sabe "
                      "cuántas piezas trae cada empaque")

    def test_retiros_no_recalcula_en_el_navegador(self):
        js = self._leer("static/retiros_internal_detail.js")
        self.assertNotIn("pesoUnit * qty", js)
        self.assertIn("data-uxv", js)


class TestNingunCalculoSueltoEnTodoElProyecto(unittest.TestCase):
    """Red de seguridad general (pedido de Daniel, 2026-08-07: "sé que es una
    columna pero puede romper cualquier cálculo si no le hacemos seguimiento").

    Barre TODO el código buscando multiplicaciones peso × cantidad-de-piezas.
    Cada una de esas es un lugar donde el flete de discos y mancuernas puede
    volver a cotizarse al doble. Si aparece una nueva, este test la caza antes
    de que llegue a una cotización real.
    """

    # Rutas revisadas una por una el 2026-08-07 y confirmadas como seguras.
    EXCEPCIONES_REVISADAS = {
        # transport_commitment_lines.peso_unitario NUNCA se escribe (solo se
        # insertan líneas de servicio ZZ, que no tienen peso): la suma da 0
        # siempre. Es un fallback muerto, no un cálculo real.
        "app.py": ["SUM(l.peso_unitario * l.cantidad)"],
        # predU acá es el peso efectivo POR PIEZA (se deriva de pred_tot/qty
        # al construir el item), así que multiplicarlo por qty es correcto.
        "templates/transporte/_modal_cotizacion_logistica.html": [
            "it.predTot = Math.round(it.predU * it.qty * 10000) / 10000"],
    }

    PATRON = re.compile(
        r"(peso|vol|pred|kg|pv)[A-Za-z_0-9]*\s*\*\s*"
        r"(qty|cant|cantidad|it\.qty|l\.cantidad)\b")

    @staticmethod
    def _sin_comentarios(contenido, es_python):
        """Devuelve las líneas con los comentarios y docstrings BLANQUEADOS
        (se conserva la numeración). Sin esto, la prosa que documenta este
        mismo bug se delata a sí misma como si fuera código."""
        lineas = contenido.splitlines()
        fuera = list(lineas)
        if es_python:
            import io as _io
            import tokenize as _tok
            try:
                toks = list(_tok.generate_tokens(
                    _io.StringIO(contenido).readline))
            except (_tok.TokenError, IndentationError, SyntaxError):
                return fuera
            for t in toks:
                # Comentarios y strings (incluye docstrings) no son código.
                if t.type in (_tok.COMMENT, _tok.STRING):
                    for ln in range(t.start[0], t.end[0] + 1):
                        if 1 <= ln <= len(fuera):
                            fuera[ln - 1] = ""
            return fuera
        # JS / HTML: máquina de estados mínima para /* */, // y <!-- -->
        en_bloque = False
        for i, linea in enumerate(lineas):
            out, j, n = [], 0, len(linea)
            while j < n:
                if en_bloque:
                    fin = linea.find("*/", j)
                    fin_html = linea.find("-->", j)
                    if fin == -1 and fin_html == -1:
                        j = n
                    else:
                        cand = [x for x in (fin, fin_html) if x != -1]
                        j = min(cand) + (2 if min(cand) == fin else 3)
                        en_bloque = False
                    continue
                if linea.startswith("//", j):
                    break
                if linea.startswith("/*", j) or linea.startswith("<!--", j):
                    en_bloque = True
                    j += 4 if linea.startswith("<!--", j) else 2
                    continue
                out.append(linea[j])
                j += 1
            fuera[i] = "".join(out)
        return fuera

    def test_no_hay_multiplicaciones_peso_por_piezas_sin_revisar(self):
        import os
        raiz = os.getcwd()
        sospechosos = []
        for base, dirs, files in os.walk(raiz):
            dirs[:] = [d for d in dirs
                       if d not in ("node_modules", ".git", "tests", "__pycache__",
                                    "worktrees", ".claude", "venv", ".venv")]
            for fn in files:
                if not fn.endswith((".py", ".js", ".html")):
                    continue
                ruta = os.path.relpath(os.path.join(base, fn), raiz).replace("\\", "/")
                try:
                    with open(os.path.join(base, fn), encoding="utf-8", errors="ignore") as fh:
                        contenido = fh.read()
                except OSError:
                    continue
                permitidas = self.EXCEPCIONES_REVISADAS.get(ruta, [])
                codigo = self._sin_comentarios(contenido, fn.endswith(".py"))
                for n, linea in enumerate(codigo, 1):
                    if not self.PATRON.search(linea):
                        continue
                    if any(p in linea for p in permitidas):
                        continue
                    sospechosos.append(f"{ruta}:{n}: {linea.strip()[:110]}")

        self.assertFalse(sospechosos,
                         "Hay cálculos de peso × piezas sin revisar. Cada uno puede "
                         "cotizar el flete al doble en discos/mancuernas. Revisa si "
                         "debe usar los empaques (unidad 2 del ERP) y, si de verdad "
                         "es correcto, agrégalo a EXCEPCIONES_REVISADAS explicando "
                         "por qué:\n  " + "\n  ".join(sospechosos))


class TestCubicajeSinLineasZZ(unittest.TestCase):
    """Las lineas ZZ (servicios) fuera del cubicaje, conservando el monto.

    Daniel, 2026-08-07: "en las cubicaciones deja limpio de ZZ, ya que son
    servicios no productos, sin embargo podemos conservar el valor de ZZ Envio".

    ZZENVIO / ZZINSTALACION / etc. son servicios facturados: no tienen peso,
    volumen ni bultos. Aparecian como una fila "s/f - Cargar medidas" pidiendo
    medidas de algo que no es carga, y su cantidad sumaba al total de UNIDADES
    como si fueran piezas fisicas (18 en vez de 17 en la FCV 11225).

    Lo que NO puede pasar: que al sacar la fila se pierda el MONTO del ZZ
    Envio, que es lo que ILUS le cobro al cliente por el despacho y sostiene
    todo el calculo de margen.
    """

    def _leer(self, ruta):
        with open(ruta, encoding="utf-8", errors="ignore") as fh:
            return fh.read()

    def test_la_grilla_excluye_las_lineas_zz(self):
        html = self._leer("templates/cubicador/index.html")
        self.assertIn("rejectattr('es_zz')", html,
                      "la grilla del Cubicador volvio a mostrar lineas de servicio")

    def test_el_monto_del_zz_envio_se_lee_de_la_lista_completa(self):
        """El bug mas facil de introducir acá: dejar el buscador de ZZENVIO
        iterando la lista YA filtrada -> el monto queda en $0 para siempre."""
        html = self._leer("templates/cubicador/index.html")
        self.assertIn("lineas_todas", html)
        i_ns = html.find("ns_zz = namespace")
        self.assertGreater(i_ns, 0, "desaparecio el calculo del ZZ Envio")
        bloque = html[i_ns:i_ns + 400]
        self.assertIn("for l in lineas_todas", bloque,
                      "el ZZ Envio se busca en la lista sin ZZ: siempre daria $0")

    def test_el_payload_de_exportacion_excluye_zz_pero_guarda_el_monto(self):
        src = APP_SRC
        i = src.find("def _cubicador_export_payload")
        self.assertGreater(i, 0)
        bloque = src[i:i + 5000]
        self.assertIn("if not l.get(\"es_zz\")", bloque,
                      "el Excel/PDF volveria a traer las lineas de servicio")
        # zzenvio por documento se calcula ANTES del filtro
        self.assertLess(bloque.find("ZZENVIO"), bloque.find('if not l.get("es_zz")'),
                        "el monto del ZZ Envio se calcula DESPUES del filtro -> quedaria en 0")

    def test_el_pdf_excluye_zz_y_muestra_el_monto(self):
        src = APP_SRC
        i = src.find("def _cubicador_pdf_response_ilus")
        self.assertGreater(i, 0)
        bloque = src[i:i + 12000]
        self.assertIn("zz_envio_monto", bloque)
        self.assertIn("if not l.get(\"es_zz\")", bloque,
                      "el PDF volvio a listar servicios como si fueran carga")
        self.assertIn("_fila_zz_envio", bloque,
                      "el monto del ZZ Envio no se muestra en el informe")

    def test_asignar_y_cotizar_sigue_filtrando_zz(self):
        # Esta pantalla YA filtraba; el cambio la deja consistente con el
        # Cubicador. Si alguien quita este filtro, vuelven a divergir.
        self.assertIn('if l.get("es_zz"):', APP_SRC)


class TestCubicajeSinLineaDeDescuento(unittest.TestCase):
    """La línea de DESCUENTO (SKU 'DE') fuera del cubicaje, igual que ZZ.

    Daniel, 2026-08-08, en respuesta directa a "¿el Cubicador debe filtrar
    también las líneas de DESCUENTO, como ya hace Asignar y Cotizar?": "Si
    también!".

    'DESCUENTO VENTAS' no es un ítem físico: no se cubica ni se envía, y su
    "cantidad" es el MONTO del descuento, no piezas — inflaba UNIDADES con un
    número que no representa nada físico. "Asignar y Cotizar" ya lo filtraba
    desde 2026-06-14; el Cubicador era el inconsistente, igual que pasaba con
    ZZ antes de ese arreglo.

    A diferencia de ZZ Envío, acá NO hay un monto que rescatar: el descuento
    ya está reflejado en total_neto/total_bruto del header (vienen del ERP
    aparte), así que sacar la línea no pierde ningún dato mostrado.
    """

    def _leer(self, ruta):
        with open(ruta, encoding="utf-8", errors="ignore") as fh:
            return fh.read()

    def test_es_descuento_se_calcula_en_la_fuente(self):
        i = APP_SRC.find("es_zz        = sku.startswith(\"ZZ\")")
        self.assertGreater(i, 0, "se movió/renombró es_zz -- ajustar el ancla")
        bloque = APP_SRC[i:i + 700]
        self.assertIn('es_descuento = sku == "DE"', bloque)

    def test_la_linea_expone_es_descuento_al_resto_del_sistema(self):
        self.assertIn('"es_descuento":        es_descuento,', APP_SRC)

    def test_la_grilla_excluye_la_linea_de_descuento(self):
        html = self._leer("templates/cubicador/index.html")
        self.assertIn("rejectattr('es_descuento')", html,
                      "la grilla del Cubicador volvió a mostrar la línea de descuento")

    def test_el_payload_de_exportacion_excluye_descuento(self):
        i = APP_SRC.find("def _cubicador_export_payload")
        self.assertGreater(i, 0)
        bloque = APP_SRC[i:i + 5000]
        self.assertIn('not l.get("es_descuento")', bloque,
                      "el Excel/PDF volvería a traer el monto del descuento "
                      "mezclado con piezas reales")

    def test_el_pdf_excluye_descuento(self):
        i = APP_SRC.find("def _cubicador_pdf_response_ilus")
        self.assertGreater(i, 0)
        bloque = APP_SRC[i:i + 12000]
        self.assertIn('not l.get("es_descuento")', bloque)

    def test_asignar_y_cotizar_sigue_filtrando_descuento(self):
        # Esta pantalla YA filtraba (2026-06-14); el cambio la deja
        # consistente con el Cubicador.
        self.assertIn('if (l.get("sku") or "").strip().upper() == "DE":', APP_SRC)

    def test_zz_y_descuento_no_se_confunden_entre_si(self):
        # Guarda contra un copy-paste descuidado: cada bandera debe depender
        # de SU propia condición, no heredar la del otro filtro.
        i = APP_SRC.find("es_zz        = sku.startswith(\"ZZ\")")
        bloque = APP_SRC[i:i + 400]
        self.assertNotIn('es_descuento = sku.startswith("ZZ")', bloque)
        self.assertNotIn('es_zz        = sku == "DE"', bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

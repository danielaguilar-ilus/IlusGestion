"""
2026-08-22. Pantalla "Despacho al courier": 6 pasos numerados con semaforo.
Pedido de Daniel: "enumerado y con semaforo, indicando que todo esta bien
segun los pasos que vas haciendo", con la referencia visual de Retiros.

LA REGLA QUE ESTA PANTALLA EXISTE PARA SOSTENER:
un paso solo se pinta VERDE cuando ILUS puede demostrar la condicion con un
dato. Si no se pudo comprobar -> "sin_comprobar", JAMAS verde.

Por que importa: durante meses la ficha mostro "Ya subido a SimpliRoute" con
un check verde. Medido contra la API real el 20-08-2026, de 15 visitas
creadas por ILUS CERO se entregaron nunca. "Subida" (paso 4) y "el courier la
planifico" (paso 5) son dos cosas distintas y esta pantalla las separa.

Estos tests son de ESTRUCTURA y de LOGICA PURA (sin BD): verifican el codigo
por AST y la semantica de los predicados reimplementandolos literal. No
pueden tocar produccion.
"""
import ast
import os
import re
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
    """ast.unparse normaliza comillas dobles a simples."""
    return s.replace('"', "'")


class TestRutasNuevasExisten(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tree = _tree()

    def _decoradores(self, nombre):
        return [ast.unparse(d) for d in _cuerpo(nombre, self.tree).decorator_list]

    def test_existe_la_pantalla(self):
        decs = _norm(" ".join(self._decoradores("tr_manifiesto_despacho")))
        self.assertIn("/transporte/manifiestos/<int:mid>/despacho", decs)

    def test_existe_el_endpoint_de_estado(self):
        decs = _norm(" ".join(self._decoradores("tr_manifiesto_despacho_estado")))
        self.assertIn("/transporte/api/manifiestos/<int:mid>/despacho/estado", decs)

    def test_ambas_exigen_permiso(self):
        for fn in ("tr_manifiesto_despacho", "tr_manifiesto_despacho_estado"):
            self.assertIn("_tr_required", " ".join(self._decoradores(fn)),
                           f"{fn} sin @_tr_required")

    def test_el_endpoint_de_estado_es_solo_lectura(self):
        """No escribe en MySQL. Un diagnostico que muta datos deja de ser
        diagnostico."""
        src = _norm(_fuente("tr_manifiesto_despacho_estado", self.tree)).upper()
        for prohibido in ("MYSQL_EXECUTE", "INSERT INTO", "UPDATE ", "DELETE FROM",
                           "CONN.COMMIT"):
            self.assertNotIn(prohibido, src,
                              f"el endpoint de estado no debe contener {prohibido}")

    def test_no_escribe_en_el_erp_random(self):
        """REGLA #4.1: el ERP es READ-ONLY ABSOLUTO."""
        src = _fuente("tr_manifiesto_despacho_estado", self.tree)
        self.assertNotIn("_random_sql", src)
        self.assertNotIn("_tr_fetch_from_erp", src)


class TestReusaLosChokePointsExistentes(unittest.TestCase):
    """No se abre un canal HTTP nuevo hacia SimpliRoute: se pasa por
    _simpliroute_request, que ya centraliza token, timeout y errores."""

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente("tr_manifiesto_despacho_estado")

    def test_usa_simpliroute_request(self):
        self.assertIn("_simpliroute_request(", self.src)

    def test_no_usa_requests_directo(self):
        n = _norm(self.src)
        for crudo in ("requests.get(", "requests.post(", "urllib.request",
                       "http.client"):
            self.assertNotIn(crudo, n, f"no debe llamar {crudo} directo")

    def test_usa_el_predicado_oficial_de_visita(self):
        self.assertIn("_sr_visita_sin_entregar(", self.src)

    def test_usa_los_helpers_de_courier(self):
        self.assertIn("_simpliroute_courier_integra(", self.src)
        self.assertIn("_simpliroute_token_for_courier(", self.src)

    def test_solo_hace_GET_a_simpliroute(self):
        """Una pantalla de diagnostico jamas debe crear ni borrar visitas."""
        for m in re.findall(r"_simpliroute_request\(\s*'([A-Z]+)'", _norm(self.src)):
            self.assertEqual(m, "GET", f"metodo {m} en una pantalla de solo lectura")


class TestNuncaVerdeSinPrueba(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("tr_manifiesto_despacho_estado"))

    def test_sin_live_el_paso5_queda_sin_comprobar(self):
        """Si no le preguntamos al courier, NO sabemos. El paso 5 nunca puede
        salir verde por defecto."""
        i = self.src.index("elif not live:")
        fragmento = self.src[i:i + 260]
        self.assertIn("sin_comprobar", fragmento)
        self.assertNotIn("'ok'", fragmento)

    def test_una_visita_que_no_respondio_no_cuenta_como_ok(self):
        i = self.src.index("if not r.get('ok'):")
        fragmento = self.src[i:i + 420]
        self.assertIn("sin_comprobar", fragmento)
        self.assertIn("consultada_ok': False", fragmento)
        self.assertIn("sin_respuesta += 1", fragmento)

    def test_el_paso4_no_se_llama_entregado_ni_despachado(self):
        """Subida != despachada. El texto del paso 4 lo dice explicito."""
        i = self.src.index("Subidas {total} de {total}")
        fragmento = self.src[i:i + 320]
        self.assertIn("Subida no es lo mismo que despachada", fragmento)

    def test_el_numero_de_documento_va_sin_ceros_de_relleno(self):
        """Pedido de Daniel (05-08): "la factura tiene cualquier cero, seria
        ideal que no tuviera". Se reusa _doc_label, el helper que ya existe,
        en vez de concatenar tido+nudo crudo -- asi esta pantalla dice el
        numero igual que el resto de la app ("FCV 11329", no
        "FCV 0000011329")."""
        self.assertIn("_doc_label(", self.src)
        self.assertNotIn("'doc': f'{it.get(", self.src)

    def test_el_paso6_solo_verde_con_Entregado(self):
        i = self.src.index("n_entregados = sum(")
        fragmento = self.src[i:i + 160]
        self.assertIn("'Entregado'", fragmento)
        # 'Entregado a transporte' NO cuenta como entrega al cliente.
        self.assertNotIn("Entregado a transporte", fragmento)


class TestSemanticaDelPeor(unittest.TestCase):
    """_dsp_peor decide el estado del manifiesto completo. 4 verdes y 3
    trabados NO es un manifiesto verde."""

    @staticmethod
    def _peor(estados):
        for e in ("trabado", "sin_comprobar", "esperando", "ahora", "pendiente", "ok"):
            if e in estados:
                return e
        return "pendiente"

    def test_un_trabado_manda_sobre_todos_los_ok(self):
        self.assertEqual(self._peor(["ok", "ok", "ok", "ok", "trabado"]), "trabado")

    def test_sin_comprobar_gana_a_esperando_y_ok(self):
        self.assertEqual(self._peor(["ok", "esperando", "sin_comprobar"]), "sin_comprobar")

    def test_trabado_gana_a_sin_comprobar(self):
        self.assertEqual(self._peor(["sin_comprobar", "trabado"]), "trabado")

    def test_todos_ok_es_ok(self):
        self.assertEqual(self._peor(["ok", "ok"]), "ok")

    def test_lista_vacia_no_revienta(self):
        self.assertEqual(self._peor([]), "pendiente")


class TestTraduccionDeEstadosDelCourier(unittest.TestCase):
    """REGLA #6/#0: nunca sale una palabra en ingles a pantalla."""

    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_app = f.read()

    def test_los_6_estados_de_simpliroute_estan_mapeados(self):
        i = self.src_app.index("_SR_ESTADO_ES = {")
        bloque = self.src_app[i:i + 700]
        for crudo in ("pending", "on_its_way", "completed", "failed",
                       "partial", "canceled"):
            self.assertIn(f'"{crudo}"', bloque, f"falta traducir {crudo}")

    def test_ninguna_traduccion_deja_ingles(self):
        i = self.src_app.index("_SR_ESTADO_ES = {")
        bloque = self.src_app[i:self.src_app.index("}", i)]
        for palabra in ("pending", "completed", "failed", "canceled"):
            # la palabra puede estar como CLAVE, pero nunca como VALOR
            for linea in bloque.splitlines():
                if ":" not in linea:
                    continue
                valor = linea.split(":", 1)[1]
                self.assertNotIn(palabra, valor.lower(),
                                  f"'{palabra}' quedo en el texto visible: {linea}")

    def test_un_estado_desconocido_no_se_muestra_como_verde(self):
        src = _fuente("_dsp_estado_courier_es")
        self.assertIn("no reconocido", _norm(src))


class TestFechasEnFormatoChile(unittest.TestCase):
    """REGLA #6: toda fecha viaja dos veces (iso + label dd/mm/aaaa) y el
    front solo imprime el label."""

    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("tr_manifiesto_despacho_estado"))
        with open("static/transporte_despacho.js", encoding="utf-8") as f:
            cls.js = f.read()

    def test_el_backend_manda_los_labels(self):
        for campo in ("hoy_label", "fecha_label", "planned_date_label"):
            self.assertIn(campo, self.src, f"falta {campo}")

    def test_los_labels_usan_dd_mm_aaaa(self):
        self.assertIn("%d/%m/%Y", self.src)

    def test_usa_la_hora_chile_no_utcnow(self):
        self.assertIn("_now_chile()", self.src)
        self.assertNotIn("utcnow", self.src)

    def test_el_js_no_construye_Date_con_el_iso(self):
        """new Date('2026-08-18') se interpreta como UTC: en Chile puede
        imprimir el dia anterior. El JS solo debe imprimir el _label.

        Se miran solo las lineas de CODIGO: el comentario de cabecera
        menciona `new Date(` justamente para explicar por que no se usa, y
        un grep ingenuo sobre el archivo entero lo confunde con una
        violacion."""
        sin_bloques = re.sub(r"/\*.*?\*/", "", self.js, flags=re.S)
        codigo = "\n".join(l for l in sin_bloques.splitlines()
                            if not l.strip().startswith("//"))
        self.assertNotIn("new Date(", codigo)


class TestSeguridadDelFrontend(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("static/transporte_despacho.js", encoding="utf-8") as f:
            cls.js = f.read()

    def test_escapa_lo_que_viene_del_servidor(self):
        """Nombres de cliente y errores del courier son texto libre."""
        self.assertIn("function esc(", self.js)
        self.assertIn("&amp;", self.js)
        self.assertIn("&lt;", self.js)

    def test_no_usa_alert_confirm_prompt_nativos(self):
        """REGLA #1."""
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(nativo, self.js, f"{nativo} nativo prohibido")

    def test_el_filtro_se_limpia_al_reclicar_el_chip_activo(self):
        """REGLA #4.3, bug real que Daniel reporto el 01-08: quitaba el
        filtro y la lista se quedaba con el resultado anterior."""
        self.assertIn("filtro = filtro === k ? null : k", self.js.replace("(", "").replace(")", ""))


class TestCssMobileYAccesibilidad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("static/transporte_despacho.css", encoding="utf-8") as f:
            cls.css = f.read()

    def test_botones_44px(self):
        """REGLA #3, Apple HIG."""
        self.assertIn("min-height: 44px", self.css)

    def test_los_chips_de_filtro_tambien_son_44px(self):
        """Medido en un viewport real de 385px daban 36px. Los chips se tocan
        con el dedo igual que cualquier boton -- mismo defecto que Daniel
        detecto en el panel de visitas congeladas (PR #166)."""
        i = self.css.index(".dsp-chip {")
        bloque = self.css[i:self.css.index("}", i)]
        self.assertIn("min-height: 44px", bloque)
        self.assertNotIn("min-height: 36px", bloque)

    def test_tiene_bloque_mobile(self):
        self.assertIn("@media (max-width: 767px)", self.css)

    def test_respeta_reduced_motion(self):
        self.assertIn("prefers-reduced-motion", self.css)

    def test_usa_la_paleta_ilus(self):
        """REGLA #2."""
        self.assertIn("#dc2626", self.css)
        self.assertIn("#16a34a", self.css)

    def test_el_estado_sin_comprobar_se_distingue_sin_color(self):
        """Un semaforo que solo cambia de color no sirve para quien no
        distingue rojo de verde: 'sin comprobar' lleva borde punteado."""
        i = self.css.index(".dsp-circle.is-unknown")
        self.assertIn("dashed", self.css[i:i + 160])


class TestNoSeRompioNadaExistente(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_local = f.read()
        cls.tree_local = ast.parse(cls.src_local)
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    # Las 4 funciones que SON esta pantalla. Tocarlas es su propio trabajo;
    # tocar cualquier otra cosa de app.py no lo es.
    PROPIAS = {
        "_dsp_estado_courier_es", "_dsp_peor",
        "tr_manifiesto_despacho", "tr_manifiesto_despacho_estado",
    }

    def test_no_toca_funciones_de_otras_features(self):
        """Esta pantalla no pisa codigo ajeno.

        AJUSTE 2026-08-22 (tercera vez que caigo en el mismo test fragil):
        antes esto decia `cambiadas == []`, o sea "no cambio NINGUNA funcion
        que exista en origin/main". Eso solo es cierto MIENTRAS el PR esta
        abierto: apenas se mergea, las funciones de esta misma pantalla pasan
        a estar en main, y el primer ajuste posterior -- aunque sea corregir
        el formato del numero de documento en su PROPIO endpoint -- la marca
        como "funcion existente modificada".

        El invariante durable no es "no cambies nada", es "no cambies nada
        que no sea tuyo". Eso es lo que se mide aca."""
        f_local = {n.name: ast.unparse(n) for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        f_main = {n.name: ast.unparse(n) for n in ast.walk(self.tree_main)
                  if isinstance(n, ast.FunctionDef)}
        cambiadas = {n for n, s in f_local.items()
                     if n in f_main and f_main[n] != s}
        ajenas = sorted(cambiadas - self.PROPIAS)
        self.assertEqual(ajenas, [], f"se modificaron funciones ajenas: {ajenas}")

    def test_las_funciones_de_la_pantalla_existen_todas(self):
        """Las 4 estan presentes, vengan de este PR o ya mergeadas."""
        f_local = {n.name for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        faltan = sorted(self.PROPIAS - f_local)
        self.assertEqual(faltan, [], f"faltan funciones de la pantalla: {faltan}")

    def test_el_cron_sigue_intacto(self):
        for fn in ("_tr_bulk_sync_erp_mysql", "_transporte_scheduler_loop"):
            self.assertEqual(_fuente(fn, self.tree_local), _fuente(fn, self.tree_main))

    def test_el_boton_viejo_de_simpliroute_sigue_existiendo(self):
        """REGLA #4.2: el boton nuevo se AGREGA al lado, no reemplaza al que
        ya estaba."""
        with open("templates/transporte/manifiesto_detalle.html", encoding="utf-8") as f:
            html = f.read()
        self.assertIn('id="btnSubirSR"', html)
        self.assertIn("subirASimpliRoute()", html)
        self.assertIn('id="btnAvisarCourier"', html)
        self.assertIn('id="btnVerDespacho"', html)

    def test_el_link_nuevo_apunta_a_la_ruta_nueva(self):
        with open("templates/transporte/manifiesto_detalle.html", encoding="utf-8") as f:
            html = f.read()
        self.assertIn("url_for('tr_manifiesto_despacho', mid=manifiesto.id)", html)


class TestArchivosDelFrontendExisten(unittest.TestCase):
    def test_los_3_archivos_nuevos(self):
        for p in ("templates/transporte/manifiesto_despacho.html",
                   "static/transporte_despacho.css",
                   "static/transporte_despacho.js"):
            self.assertTrue(os.path.exists(p), f"falta {p}")

    def test_el_template_carga_su_css_y_su_js(self):
        with open("templates/transporte/manifiesto_despacho.html", encoding="utf-8") as f:
            html = f.read()
        self.assertIn("transporte_despacho.css", html)
        self.assertIn("transporte_despacho.js", html)

    def test_no_pone_version_manual_en_static(self):
        """El cache-busting es automatico via @app.url_defaults. Un ?v= a
        mano lo rompe."""
        with open("templates/transporte/manifiesto_despacho.html", encoding="utf-8") as f:
            html = f.read()
        self.assertNotIn("?v=", html)


if __name__ == "__main__":
    unittest.main()

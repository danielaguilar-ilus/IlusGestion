"""30-oct-2026 (Daniel, COT-000195 de PHYSIOCORE HF SPA): el PDF decía «6 equipos considerados» y «Instalación de 6 equipo(s)», pero era UN
servicio («Instalación cable de fuerza») más 5 unidades de un material («Cable de Fuerza 5mm Black»). El cliente reclamó con razón.
Se prueba el módulo puro `cot_composicion` con el caso real y con los casos con equipos, y que tickets_module lo use.
Correr con: python -m unittest tests.test_cot_composicion"""
import os
import unittest

import cot_composicion as C

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _it(sku, desc, cant, clase=None):
    return {"sku": sku, "descripcion": desc, "cantidad": cant, "clase_producto": clase}


COT_195 = [_it("", "Cable de Fuerza 5mm Black", 5), _it("", "Instalación cable de fuerza", 1)]


class TestClasificar(unittest.TestCase):
    def test_servicio_material_equipo(self):
        self.assertEqual(C.clasificar_linea("ZZINSTALACION", "Servicio", None), C.SERVICIO)
        self.assertEqual(C.clasificar_linea("", "Instalación cable de fuerza"), C.SERVICIO)
        self.assertEqual(C.clasificar_linea("DE", "DESCUENTO VENTAS"), C.SERVICIO)
        self.assertEqual(C.clasificar_linea("", "Cable de Fuerza 5mm Black"), C.MATERIAL)
        self.assertEqual(C.clasificar_linea("X1", "Soporte pared", "accesorio"), C.MATERIAL)
        self.assertEqual(C.clasificar_linea("R1", "Rodamiento", "repuesto"), C.MATERIAL)
        self.assertEqual(C.clasificar_linea("TRO1", "Trotadora ILUS T-900", None), C.EQUIPO)
        self.assertEqual(C.clasificar_linea("", "INSTALACIÓN y armado", None), C.SERVICIO, "mayúsculas y acentos")


class TestCaso195(unittest.TestCase):
    def setUp(self):
        self.c = C.componer(COT_195)

    def test_no_son_6_equipos(self):
        self.assertEqual(self.c["equipos"], 0)
        self.assertEqual(self.c["materiales"], 5)
        self.assertEqual(self.c["servicios"], 1)
        self.assertTrue(self.c["solo_servicio"])

    def test_las_tarjetas_dicen_lo_que_hay(self):
        lbls = {k["lbl"]: k["num"] for k in self.c["cards"]}
        self.assertEqual(lbls, {"Servicio considerado": 1, "de material": "5 m"}, "el cable se cuenta en metros")
        self.assertNotIn("Equipos considerados", lbls)

    def test_el_subtitulo_del_detalle(self):
        self.assertEqual(self.c["detalle_sub"], "1 servicio · 5 m de material · precios unitarios en UF")

    def test_los_textos_no_hablan_de_equipos_instalados(self):
        parrafo, alcance, reco, servicio = C.textos_instalacion(self.c, uf_info=True)
        todo = " ".join([parrafo, servicio] + alcance + reco).lower()
        self.assertNotIn("6 equipo", todo)
        self.assertNotIn("equipo(s)", todo)
        self.assertNotIn("cada equipo", todo)
        self.assertIn("instalación cable de fuerza", todo)
        self.assertIn("cable de fuerza 5mm black (5 m)", todo)
        self.assertIn("uf vigente", todo)
        self.assertEqual(len(reco), 4)


class TestConEquipos(unittest.TestCase):
    ITEMS = [_it("TRO1", "Trotadora ILUS T-900", 2, "trotadora"), _it("BIC1", "Bicicleta spinning", 1, "bicicleta"),
             _it("ZZINSTALACION", "Instalación", 1), _it("", "Cable de fuerza", 3)]

    def test_los_servicios_y_materiales_no_inflan_los_equipos(self):
        c = C.componer(self.ITEMS, dias_habiles=2)
        self.assertEqual((c["equipos"], c["categorias"], c["servicios"], c["materiales"]), (3, 2, 1, 3))
        self.assertEqual([k["lbl"] for k in c["cards"]],
                         ["Equipos considerados", "Categorías de equipos", "Servicio considerado", "de material",
                          "Días hábiles estimados"])
        self.assertEqual(c["detalle_sub"], "3 equipos · 1 servicio · 3 m de material · precios unitarios en UF")

    def test_con_equipos_conserva_la_redaccion_de_siempre(self):
        c = C.componer(self.ITEMS)
        parrafo, alcance, reco, servicio = C.textos_instalacion(c, previas=2)
        self.assertEqual(alcance[0], "Instalación de 3 equipos en las dependencias del cliente.")
        self.assertIn("Suministro de los materiales", alcance[1])
        self.assertIn("instalación de los equipos individualizados", parrafo)
        self.assertTrue(servicio.startswith("instalación, armado, nivelación"))
        self.assertTrue(reco[0].startswith("Cliente recurrente: ya registra 2"))
        self.assertIn("Registrar número de serie", reco[1])

    def test_un_solo_equipo_va_en_singular(self):
        c = C.componer([_it("TRO1", "Trotadora", 1)])
        self.assertEqual(C.textos_instalacion(c)[1][0], "Instalación de 1 equipo en las dependencias del cliente.")
        self.assertEqual(c["detalle_sub"], "1 equipo · precios unitarios en UF")

    def test_lineas_en_cero_o_basura_no_cuentan(self):
        c = C.componer([_it("TRO1", "Trotadora", 0), _it("TRO2", "Elíptica", "x"), {}])
        self.assertEqual((c["equipos"], c["servicios"], c["materiales"]), (0, 0, 0))
        self.assertIsNone(c["detalle_sub"])


class TestMetros(unittest.TestCase):
    """Daniel (30-oct, COT-000195): «que diga metros en el cable»."""

    def test_unidad_de(self):
        self.assertEqual(C.unidad_de("Cable de Fuerza 5mm Black"), "m")
        self.assertEqual(C.unidad_de("Piola de acero 6mm"), "m")
        self.assertEqual(C.unidad_de("Cuerda de batalla"), "m")
        self.assertEqual(C.unidad_de("Cable USB para consola"), "", "un cable USB se cuenta en unidades")
        self.assertEqual(C.unidad_de("Cable de poder"), "")
        self.assertEqual(C.unidad_de("Instalación cable de fuerza"), "", "un servicio nunca lleva metros")
        self.assertEqual(C.unidad_de("Cable", "ZZINSTALACION"), "")
        self.assertEqual(C.unidad_de("Trotadora ILUS"), "")

    def test_material_mixto_cuenta_unidades(self):
        c = C.componer([_it("", "Cable de fuerza", 5), _it("", "Perno anclaje", 4), _it("ZZINSTALACION", "Instalación", 1)])
        self.assertEqual(c["detalle_sub"], "1 servicio · 9 unidades de material · precios unitarios en UF")

    def test_el_pdf_muestra_5_m_en_la_columna_unidades(self):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates", "tickets")), autoescape=True)
        env.globals["clp"] = lambda v: str(v)
        env.filters["rut_fmt"] = lambda v: v
        items = [{"sku": "", "descripcion": "Cable de Fuerza 5mm Black", "cantidad": 5, "unidad": "m", "precio_uf": "0,3", "total": 59950,
                  "documento": "—", "documento_tido": "", "clase_producto": None, "es_cobrable": True, "precio_unitario": 11990},
                 {"sku": "", "descripcion": "Instalación cable de fuerza", "cantidad": 1, "unidad": None, "precio_uf": "1,8", "total": 73020,
                  "documento": "—", "documento_tido": "", "clase_producto": None, "es_cobrable": True, "precio_unitario": 73020}]
        cot = {"id": 1, "numero": "COT-1", "fecha_emision": "", "empresa": "", "rut": "", "direccion": "", "comuna": "", "region": "", "email": "",
               "telefono": "", "ejecutivo": "", "contacto_nombre": "", "tipo_servicio_label": "Instalación", "subtotal": 0, "descuento_pct": 0,
               "descuento_monto": 0, "costo_ruta": 0, "iva_pct": 19, "iva_monto": 0, "total": 0}
        html = env.get_template("cotizacion_pdf.html").render(cot=cot, items=items, modo_rico=False, mostrar_col_documento=False,
                                                              terminos_pares=[], uf_info=None, kpis=None, rico_textos=None)
        import re
        celdas = re.findall(r'<td class="cant">([^<]*)</td>', html)
        self.assertEqual([c.strip() for c in celdas], ["5 m", "1"])

# Las líneas REALES (nombre y clase del categorizador) de las cotizaciones de producción, leídas el 4-nov-2026.
COT_203_FREEMOTION = _it("G624-03", "Freemotion Dual Cable Cross Negra", 1, "dual_cable_cross")
COT_195_REAL = [_it("RE05SDP176", "Cable de Fuerza 5mm Black", 5, "repuesto"), _it("INS", "Instalación cable de fuerza", 1, "selectorizador_pesos")]


class TestClaseDelCategorizador(unittest.TestCase):
    """Daniel (4-nov, COT-000203): Aarón vio «metros de cable» en un EQUIPO. El nombre de un equipo puede llevar «cable» o «cinta» sin ser un
    material; el categorizador del Catálogo (clase_producto) ya sabe qué es: si dice equipo, es un equipo."""

    def test_el_equipo_de_la_cot_203_no_lleva_metros_ni_es_material(self):
        self.assertEqual(C.unidad_de(COT_203_FREEMOTION["descripcion"], COT_203_FREEMOTION["sku"], COT_203_FREEMOTION["clase_producto"]), "")
        self.assertEqual(C.clasificar_linea("G624-03", "Freemotion Dual Cable Cross Negra", "dual_cable_cross"), C.EQUIPO)

    def test_sigue_igual_la_cot_195_que_a_daniel_le_encantaba(self):
        """Con la clase real (repuesto) el cable sigue en metros y el servicio sigue siendo servicio, aunque la instalación tenga clase de equipo."""
        c = C.componer(COT_195_REAL)
        self.assertEqual({k["lbl"]: k["num"] for k in c["cards"]}, {"Servicio considerado": 1, "de material": "5 m"})
        self.assertEqual(c["detalle_sub"], "1 servicio · 5 m de material · precios unitarios en UF")
        self.assertEqual(C.unidad_de("Cable de Fuerza 5mm Black", "RE05SDP176", "repuesto"), "m")
        self.assertEqual(C.unidad_de("Instalación cable de fuerza", "INS", "selectorizador_pesos"), "", "un servicio nunca lleva metros")
        self.assertEqual(C.clasificar_linea("INS", "Instalación cable de fuerza", "selectorizador_pesos"), C.SERVICIO)

    def test_una_cotizacion_de_equipos_con_un_dual_cable_cuenta_sus_equipos(self):
        c = C.componer([_it("FL1826", "ILUS Leg Press Unilateral 35 Grados Black", 1, "selectorizador_pesos"), COT_203_FREEMOTION,
                        _it("005503BBC", "M3i Lite Indoor Bike Keiser C/ Bluethooth incorpo", 6, "bicicleta")])
        self.assertEqual((c["equipos"], c["materiales"], c["servicios"]), (8, 0, 0))
        self.assertEqual({k["lbl"]: k["num"] for k in c["cards"]}["Categorías de equipos"], 3)

    def test_un_equipo_con_cinta_en_el_nombre_no_es_material(self):
        """«Cinta» está en la lista de materiales por nombre: una trotadora llamada «Cinta de correr» se contaba como material."""
        self.assertEqual(C.clasificar_linea("T1", "Cinta de correr ILUS", "trotadora"), C.EQUIPO)
        self.assertEqual(C.clasificar_linea("T1", "Cinta de correr ILUS", None), C.MATERIAL, "sin clase decide el nombre, como siempre")

    def test_repuesto_o_accesorio_se_vende_por_metro_aunque_la_palabra_no_vaya_primera(self):
        self.assertEqual(C.unidad_de("Repuesto cable de acero 6mm", "", "repuesto"), "m")
        self.assertEqual(C.unidad_de("Piola de acero 6mm", "", "accesorio"), "m")
        self.assertEqual(C.unidad_de("Cable USB para consola", "", "repuesto"), "", "un cable USB se cuenta en unidades aunque sea repuesto")

    def test_sin_clase_util_solo_si_el_nombre_empieza_con_la_palabra(self):
        for clase in (None, "", "otro", "sin_clasificar"):
            self.assertEqual(C.unidad_de("Cable de Fuerza 5mm Black", "", clase), "m", clase)
            self.assertEqual(C.unidad_de("Dual Cable Cross Negra", "", clase), "", f"{clase!r}: la palabra va en el medio")
            self.assertEqual(C.unidad_de("Torre de cable ajustable", "", clase), "", f"{clase!r}")

    def test_cualquier_clase_nueva_del_catalogo_cuenta_como_equipo(self):
        """Si Daniel agrega una clase en /catalogo/clases, no hay que tocar el código."""
        self.assertTrue(C.es_clase_de_equipo("remo_nuevo_2027"))
        for no in (None, "", "accesorio", "repuesto", "otro", "sin_clasificar", "  Repuesto "):
            self.assertFalse(C.es_clase_de_equipo(no), repr(no))

    def test_la_clase_llega_desde_la_cotizacion_a_los_dos_lugares_donde_se_decide(self):
        with open(os.path.join(RAIZ, "tickets_module.py"), encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn('_cotcomp.unidad_de(it.get("descripcion"), it.get("erp_kopr"), it.get("clase_producto"))', t)
        with open(os.path.join(RAIZ, "cot_composicion.py"), encoding="utf-8") as fh:
            m = fh.read()
        self.assertIn('unidad_de(it.get("descripcion"), it.get("sku"), it.get("clase_producto"))', m)

    def test_el_pdf_de_la_cot_203_no_muestra_metros(self):
        import re
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates", "tickets")), autoescape=True)
        env.globals["clp"] = lambda v: str(v)
        env.filters["rut_fmt"] = lambda v: v
        items = [{"sku": "G624-03", "descripcion": "Freemotion Dual Cable Cross Negra", "cantidad": 1,
                  "unidad": C.unidad_de("Freemotion Dual Cable Cross Negra", "G624-03", "dual_cable_cross") or None, "precio_uf": "1,0",
                  "total": 1, "documento": "—", "documento_tido": "", "clase_producto": "dual_cable_cross", "es_cobrable": True, "precio_unitario": 1}]
        cot = {"id": 1, "numero": "COT-203", "fecha_emision": "", "empresa": "", "rut": "", "direccion": "", "comuna": "", "region": "", "email": "",
               "telefono": "", "ejecutivo": "", "contacto_nombre": "", "tipo_servicio_label": "Instalación", "subtotal": 0, "descuento_pct": 0,
               "descuento_monto": 0, "costo_ruta": 0, "iva_pct": 19, "iva_monto": 0, "total": 0}
        html = env.get_template("cotizacion_pdf.html").render(cot=cot, items=items, modo_rico=False, mostrar_col_documento=False,
                                                              terminos_pares=[], uf_info=None, kpis=None, rico_textos=None)
        self.assertEqual([c.strip() for c in re.findall(r'<td class="cant">([^<]*)</td>', html)], ["1"])


class TestCableado(unittest.TestCase):
    def test_tickets_module_usa_la_composicion(self):
        with open(os.path.join(RAIZ, "tickets_module.py"), encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn("import cot_composicion as _cotcomp", t)
        self.assertIn("_cotcomp.componer(items", t)
        self.assertIn("_cotcomp.textos_instalacion(", t)
        self.assertIn('"kpi_cards": (_comp_inst["cards"]', t)
        self.assertIn('"detalle_sub": _comp_inst["detalle_sub"]', t)
        self.assertNotIn("Instalación de {_n_equipos} equipo(s)", t, "el conteo de «equipos» ya no suma todas las unidades")

    def test_la_plantilla_ya_soporta_las_tarjetas_y_el_subtitulo(self):
        with open(os.path.join(RAIZ, "templates", "tickets", "cotizacion_pdf.html"), encoding="utf-8") as fh:
            h = fh.read()
        self.assertIn("{% if kpi_cards %}", h)
        self.assertIn("rico_textos.detalle_sub", h)


if __name__ == "__main__":
    unittest.main()

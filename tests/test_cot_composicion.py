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
        self.assertEqual(lbls, {"Servicio considerado": 1, "Unidades de material": 5})
        self.assertNotIn("Equipos considerados", lbls)

    def test_el_subtitulo_del_detalle(self):
        self.assertEqual(self.c["detalle_sub"], "1 servicio · 5 unidades de material · precios unitarios en UF")

    def test_los_textos_no_hablan_de_equipos_instalados(self):
        parrafo, alcance, reco, servicio = C.textos_instalacion(self.c, uf_info=True)
        todo = " ".join([parrafo, servicio] + alcance + reco).lower()
        self.assertNotIn("6 equipo", todo)
        self.assertNotIn("equipo(s)", todo)
        self.assertNotIn("cada equipo", todo)
        self.assertIn("instalación cable de fuerza", todo)
        self.assertIn("cable de fuerza 5mm black (5 u)", todo)
        self.assertIn("uf vigente", todo)
        self.assertEqual(len(reco), 4)


class TestConEquipos(unittest.TestCase):
    ITEMS = [_it("TRO1", "Trotadora ILUS T-900", 2, "trotadora"), _it("BIC1", "Bicicleta spinning", 1, "bicicleta"),
             _it("ZZINSTALACION", "Instalación", 1), _it("", "Cable de fuerza", 3)]

    def test_los_servicios_y_materiales_no_inflan_los_equipos(self):
        c = C.componer(self.ITEMS, dias_habiles=2)
        self.assertEqual((c["equipos"], c["categorias"], c["servicios"], c["materiales"]), (3, 2, 1, 3))
        self.assertEqual([k["lbl"] for k in c["cards"]],
                         ["Equipos considerados", "Categorías de equipos", "Servicio considerado", "Unidades de material",
                          "Días hábiles estimados"])
        self.assertEqual(c["detalle_sub"], "3 equipos · 1 servicio · 3 unidades de material · precios unitarios en UF")

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

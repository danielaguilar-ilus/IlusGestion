# -*- coding: utf-8 -*-
"""🚚 FLOTA · lector de licencias de conducir (parte pura): del TEXTO que devuelve el OCR a los datos, y la comparación con el registro.

Daniel, 05-nov-2026: «un scanner de licencias… que se pueda hacer por el celular o subiendo una foto… sobre todo la fecha, así evitamos que
Milagros cree choferes con su usuario. Fíjate que en ese espacio dice "fecha de control 29/10/2029"».

Todo con personas y RUT INVENTADOS (nada de datos reales). Los textos de abajo imitan lo que devuelve un OCR sobre una licencia chilena, con sus
errores típicos (O por 0, líneas partidas, etiquetas medio ilegibles).

Correr:  python -m pytest tests/test_flota_licencia_lector.py -q -p no:warnings
"""
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_licencia_lector as L  # noqa: E402
import flota_reglas as R  # noqa: E402


def rut_de(cuerpo):
    """RUT válido a partir de su cuerpo: 12345678 → '12345678-5'."""
    suma, factor = 0, 2
    for d in reversed(str(cuerpo)):
        suma += int(d) * factor
        factor = 2 if factor == 7 else factor + 1
    r = 11 - (suma % 11)
    return f"{cuerpo}-{'0' if r == 11 else ('K' if r == 10 else r)}"


MARIA = rut_de(12345678)           # 12345678-5
PEDRO = rut_de(9876543)            # otra persona
HOY = date(2026, 11, 5)

LIMPIO = """LICENCIA DE CONDUCTOR
REPÚBLICA DE CHILE
CLASE B
Nº DE LICENCIA 12.345.678-5
MUNICIPALIDAD INDEPENDENCIA
NOMBRES MARIA ELENA
APELLIDOS PEREZ SOTO
DIRECCIÓN COLON 1265 DPTO 1017 TORRE B
FECHA ÚLTIMO CONTROL 16/06/2023
FECHA DE CONTROL 29/10/2029
128189
"""

# Valores debajo de su etiqueta, la O por 0, el RUT con espacio y la foto del carnet repitiendo el nombre y el RUT.
RUIDOSO = """LICENCIA DE CONDUCTOR REPUBLICA DE CHILE
MARIA ELENA
PEREZ SOTO
12.345.678-5
CLASE B
N° DE LICENCIA 12.345.678- 5
INDEPENDENCIA
NOMBRES
MARIA ELENA
APELLIDOS
PEREZ SOTO
FECHA ULTIMO CONTROL 16/O6/2O23
FECHA DE CONTROL 29/1O/2O29
128189
"""


class TestLecturaDeUnaLicenciaLimpia(unittest.TestCase):
    def setUp(self):
        self.l = L.interpretar_texto(LIMPIO)

    def test_rut(self):
        self.assertEqual(self.l["rut"], "12345678-5")
        self.assertEqual(self.l["rut_formato"], "12.345.678-5")

    def test_la_fecha_que_importa_es_la_de_control_no_la_del_ultimo_control(self):
        self.assertEqual(self.l["vence"], date(2029, 10, 29))
        self.assertEqual(self.l["ultimo_control"], date(2023, 6, 16))

    def test_clase_y_nombre(self):
        self.assertEqual(self.l["clases"], ["B"])
        self.assertEqual(self.l["nombres"], "MARIA ELENA")
        self.assertEqual(self.l["apellidos"], "PEREZ SOTO")
        self.assertEqual(self.l["nombre"], "MARIA ELENA PEREZ SOTO")

    def test_es_legible_y_sin_avisos(self):
        self.assertTrue(self.l["legible"])
        self.assertEqual(self.l["avisos"], [])


class TestLecturaConErroresDelOCR(unittest.TestCase):
    def test_la_O_por_cero_en_las_fechas(self):
        l = L.interpretar_texto(RUIDOSO)
        self.assertEqual(l["vence"], date(2029, 10, 29))
        self.assertEqual(l["ultimo_control"], date(2023, 6, 16))

    def test_rut_con_espacio_antes_del_digito_verificador(self):
        self.assertEqual(L.interpretar_texto(RUIDOSO)["rut"], "12345678-5")

    def test_el_nombre_puede_venir_en_la_linea_de_abajo(self):
        l = L.interpretar_texto(RUIDOSO)
        self.assertEqual(l["nombre"], "MARIA ELENA PEREZ SOTO")

    def test_letras_que_parecen_numeros_en_el_rut(self):
        # «S» leída en lugar del 5 del dígito verificador y «O» en lugar de 0 en el cuerpo
        for variante in ("12.345.678-S", "I2.345.678-5", "12.345.678 - 5", "12345678-5", "12345678 5"):
            with self.subTest(rut=variante):
                self.assertEqual(L.interpretar_texto(f"N DE LICENCIA {variante}\nFECHA DE CONTROL 29/10/2029")["rut"], "12345678-5")

    def test_sin_las_palabras_ultimo_control_toma_la_fecha_mas_lejana_y_lo_avisa(self):
        l = L.interpretar_texto("12.345.678-5\n16/06/2023\n29/10/2029\nCLASE B")
        self.assertEqual(l["vence"], date(2029, 10, 29))
        self.assertIn("fecha_sin_etiqueta", l["avisos"])
        self.assertTrue(l["legible"])

    def test_las_dos_etiquetas_dicen_control_gana_la_mas_lejana(self):
        l = L.interpretar_texto("12.345.678-5\nFECHA CONTROL 16/06/2023\nFECHA DE CONTROL 29/10/2029")
        self.assertEqual(l["vence"], date(2029, 10, 29))
        self.assertNotIn("fecha_sin_etiqueta", l["avisos"])

    def test_la_fecha_con_la_etiqueta_en_la_linea_anterior(self):
        l = L.interpretar_texto("12.345.678-5\nFECHA DE CONTROL\n29/10/2029")
        self.assertEqual(l["vence"], date(2029, 10, 29))

    def test_separadores_distintos_de_la_barra(self):
        for f in ("29-10-2029", "29.10.2029", "29 / 10 / 2029"):
            with self.subTest(f=f):
                self.assertEqual(L.interpretar_texto(f"12.345.678-5\nFECHA DE CONTROL {f}")["vence"], date(2029, 10, 29))


class TestNuncaDaPorBuenaUnaLecturaDudosa(unittest.TestCase):
    def test_un_rut_con_digito_verificador_malo_no_se_acepta(self):
        l = L.interpretar_texto("N DE LICENCIA 12.345.678-9\nFECHA DE CONTROL 29/10/2029")      # el verificador correcto es 5
        self.assertIsNone(l["rut"])                               # un RUT que no existe NO se da por leído…
        self.assertIn("rut_invalido", l["avisos"])
        self.assertEqual(l["ruts_leidos"], ["12345678-9"])        # …pero queda anotado lo que se vio, para comparar con el que declara la persona
        errores, _ = L.evaluar(l, rut_chofer=MARIA, hoy=HOY)
        self.assertIn("rut", errores)                             # y 9 no es una cifra que el OCR confunda con 5: no coincide

    def test_una_fecha_imposible_no_se_acepta(self):
        l = L.interpretar_texto("12.345.678-5\nFECHA DE CONTROL 31/02/2029")
        self.assertIsNone(l["vence"])
        self.assertFalse(l["legible"])

    def test_un_anio_absurdo_no_se_acepta(self):
        self.assertIsNone(L.interpretar_texto("12.345.678-5\nFECHA DE CONTROL 29/10/1899")["vence"])

    def test_sin_fecha_no_es_legible(self):
        l = L.interpretar_texto("12.345.678-5\nCLASE B")
        self.assertFalse(l["legible"])
        self.assertIn("sin_fecha", l["avisos"])

    def test_sin_rut_no_es_legible(self):
        l = L.interpretar_texto("FECHA DE CONTROL 29/10/2029\nCLASE B")
        self.assertFalse(l["legible"])
        self.assertIn("sin_rut", l["avisos"])

    def test_texto_sin_sentido_o_vacio(self):
        for t in ("", "   ", "xxx 123 yyy", None):
            with self.subTest(t=t):
                l = L.interpretar_texto(t)
                self.assertFalse(l["legible"])
                self.assertIsNone(l["rut"])
                self.assertIsNone(l["vence"])

    def test_dos_ruts_distintos_se_avisa_y_gana_el_que_mas_veces_aparece(self):
        l = L.interpretar_texto(f"12.345.678-5\nN DE LICENCIA 12.345.678-5\n{R.rut_formato(PEDRO)}\nFECHA DE CONTROL 29/10/2029")
        self.assertEqual(l["rut"], "12345678-5")
        self.assertIn("varios_rut", l["avisos"])

    def test_el_folio_y_los_numeros_sueltos_no_son_rut(self):
        l = L.interpretar_texto("128189\n23316327\nFECHA DE CONTROL 29/10/2029")
        self.assertIsNone(l["rut"])


class TestClases(unittest.TestCase):
    def test_una_clase(self):
        self.assertEqual(L.interpretar_texto("CLASE B")["clases"], ["B"])

    def test_varias_clases_en_el_orden_oficial(self):
        self.assertEqual(L.interpretar_texto("CLASE B, A4")["clases"], ["A4", "B"])
        self.assertEqual(L.interpretar_texto("CLASES A2 A4 B D")["clases"], ["A2", "A4", "B", "D"])

    def test_se_corta_donde_termina_la_clase(self):
        self.assertEqual(L.interpretar_texto("CLASE B N DE LICENCIA 12.345.678-5")["clases"], ["B"])

    def test_sin_la_palabra_clase_no_se_inventa(self):
        l = L.interpretar_texto("B\n12.345.678-5\nFECHA DE CONTROL 29/10/2029")
        self.assertEqual(l["clases"], [])
        self.assertIn("sin_clase", l["avisos"])

    def test_A1_leida_como_AI(self):
        self.assertEqual(L.interpretar_texto("CLASE AI")["clases"], ["A1"])


class TestFusionarVariasPasadasDelOCR(unittest.TestCase):
    def test_gana_lo_que_mas_veces_se_leyo(self):
        buenas = [L.interpretar_texto(LIMPIO), L.interpretar_texto(LIMPIO), L.interpretar_texto(RUIDOSO)]
        mala = L.interpretar_texto(LIMPIO.replace("29/10/2029", "28/10/2029"))
        f = L.fusionar([mala] + buenas)
        self.assertEqual(f["vence"], date(2029, 10, 29))
        self.assertEqual(f["rut"], "12345678-5")
        self.assertTrue(f["legible"])

    def test_si_una_pasada_lee_lo_que_otra_no_se_completan(self):
        sin_fecha = L.interpretar_texto("12.345.678-5\nCLASE B")
        con_fecha = L.interpretar_texto("FECHA DE CONTROL 29/10/2029\nNOMBRES MARIA ELENA\nAPELLIDOS PEREZ SOTO")
        f = L.fusionar([sin_fecha, con_fecha])
        self.assertTrue(f["legible"])
        self.assertEqual(f["rut"], "12345678-5")
        self.assertEqual(f["vence"], date(2029, 10, 29))

    def test_sin_lecturas_es_una_lectura_vacia(self):
        self.assertFalse(L.fusionar([])["legible"])

    def test_a_igualdad_de_votos_gana_la_lectura_mas_completa(self):
        completa = L.interpretar_texto(LIMPIO)
        sin_clase_ni_nombre = LIMPIO.replace("CLASE B\n", "").replace("NOMBRES MARIA ELENA", "")
        parcial = L.interpretar_texto(sin_clase_ni_nombre.replace("29/10/2029", "27/10/2029"))
        self.assertEqual(L.fusionar([parcial, completa])["vence"], date(2029, 10, 29))
        self.assertEqual(L.fusionar([completa, parcial])["vence"], date(2029, 10, 29))


class TestNombrePorPalabras(unittest.TestCase):
    """En la licencia real las etiquetas «Nombres» y «Apellidos» (cursiva bajo un sello) no se leyeron: el nombre se comprueba con las palabras."""
    SIN_ETIQUETAS = "LICENCIA DE CONDUCTOR\n12.345.678-5\nINDEPENDENCIA\nMARIA ELENA\nPEREZ SOTO\nFECHA ULTIMO CONTROL 16/06/2023\nFECHA DE CONTROL 29/10/2029"

    def test_las_palabras_del_texto_se_guardan_sin_acentos_ni_particulas(self):
        l = L.interpretar_texto("Dirección: Colón 1265 de la Torre B\nMARIA ELENA")
        self.assertIn("MARIA", l["palabras"])
        self.assertIn("COLON", l["palabras"])
        self.assertNotIn("DE", l["palabras"])
        self.assertEqual(l["palabras"], sorted(l["palabras"]))

    def test_sin_etiquetas_el_nombre_queda_vacio_pero_el_usuario_se_comprueba_por_palabras(self):
        l = L.interpretar_texto(self.SIN_ETIQUETAS)
        self.assertIsNone(l["nombre"])
        errores, avisos = L.evaluar(l, rut_chofer=MARIA, usuario={"nombre": "María Elena Pérez", "rut": ""}, hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("no tiene RUT" in a for a in avisos))

    def test_sin_etiquetas_un_usuario_de_otra_persona_se_bloquea(self):
        l = L.interpretar_texto(self.SIN_ETIQUETAS)
        errores, _ = L.evaluar(l, rut_chofer=MARIA, usuario={"nombre": "Milagros Salazar", "rut": ""}, hoy=HOY)
        self.assertIn("usuario_id", errores)

    def test_el_nombre_escrito_distinto_avisa_tambien_con_palabras(self):
        l = L.interpretar_texto(self.SIN_ETIQUETAS)
        _, avisos = L.evaluar(l, rut_chofer=MARIA, nombre_chofer="Juan Gómez Rojas", hoy=HOY)
        self.assertTrue(any("no se parece" in a for a in avisos))
        _, avisos = L.evaluar(l, rut_chofer=MARIA, nombre_chofer="María Elena Pérez Soto", hoy=HOY)
        self.assertFalse(any("no se parece" in a for a in avisos))

    def test_la_fusion_junta_las_palabras_de_todas_las_pasadas(self):
        f = L.fusionar([L.interpretar_texto("MARIA ELENA"), L.interpretar_texto("PEREZ SOTO")])
        self.assertTrue({"MARIA", "ELENA", "PEREZ", "SOTO"} <= set(f["palabras"]))


class TestFechaPocoSegura(unittest.TestCase):
    def test_una_fecha_que_solo_vio_una_pasada_se_avisa(self):
        f = L.fusionar([L.interpretar_texto(LIMPIO), L.interpretar_texto("12.345.678-5")])
        self.assertEqual(f["vence"], date(2029, 10, 29))
        self.assertIn("fecha_poco_segura", f["avisos"])

    def test_con_dos_pasadas_de_acuerdo_no_se_avisa(self):
        f = L.fusionar([L.interpretar_texto(LIMPIO), L.interpretar_texto(RUIDOSO)])
        self.assertNotIn("fecha_poco_segura", f["avisos"])

    def test_el_aviso_llega_a_la_persona(self):
        f = L.fusionar([L.interpretar_texto(LIMPIO)])
        errores, avisos = L.evaluar(f, rut_chofer=MARIA, hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("compárala con la foto" in a for a in avisos))


# La licencia REAL leída con Tesseract (05-nov-2026) dio, según la pasada: «25.647.065-5» (un 5 leído como 6), «4 6/06/2023» y «29/10/2029»
# solo en algunas, y a veces una fecha sola sin su etiqueta. Esto se prueba con texto inventado del mismo estilo.
class TestLoQueSeVioEnLaFoto(unittest.TestCase):
    PASADA_RUT_MAL = "N DE LICENCIA 12.345.678-5\nFECHA ULTIMO CONTROL 16/06/2023"
    PASADA_FECHA = "N DE LICENCIA 12.345.678-5\nFECHA ULTIMO CONTROL 16/06/2023\nFECHA DE CONTROL 29/10/2029"

    def test_una_fecha_sola_y_sin_etiqueta_no_se_toma_por_vencimiento(self):
        l = L.interpretar_texto("12.345.678-5\n16/06/2023")
        self.assertIsNone(l["vence"])
        self.assertEqual(l["fechas"], [date(2023, 6, 16)])
        self.assertIn("una_sola_fecha", l["avisos"])
        self.assertNotIn("sin_fecha", l["avisos"])
        self.assertTrue(l["legible"])                             # hay algo que mostrar y comparar: la persona elige la fecha

    def test_una_fecha_sola_con_la_etiqueta_de_control_si_es_el_vencimiento(self):
        l = L.interpretar_texto("12.345.678-5\nFECHA DE CONTROL 29/10/2029")
        self.assertEqual(l["vence"], date(2029, 10, 29))

    def test_una_fecha_sola_con_la_etiqueta_ultimo_control_nunca_es_el_vencimiento(self):
        l = L.interpretar_texto("12.345.678-5\nFECHA ULTIMO CONTROL 16/06/2023")
        self.assertIsNone(l["vence"])
        self.assertEqual(l["ultimo_control"], date(2023, 6, 16))

    def test_cada_pasada_vio_una_fecha_distinta_y_entre_todas_alcanza(self):
        f = L.fusionar([L.interpretar_texto("12.345.678-5\n16/06/2023"), L.interpretar_texto("29/10/2029")])
        self.assertEqual(f["fechas"], [date(2023, 6, 16), date(2029, 10, 29)])
        self.assertEqual(f["vence"], date(2029, 10, 29))          # la de control es la más lejana
        self.assertIn("fecha_sin_etiqueta", f["avisos"])
        self.assertIn("fecha_poco_segura", f["avisos"])           # y solo una pasada la vio: se le dice a la persona que la compare
        self.assertNotIn("una_sola_fecha", f["avisos"])

    def test_si_en_todas_las_pasadas_solo_aparece_una_fecha_no_se_inventa_la_otra(self):
        f = L.fusionar([L.interpretar_texto("12.345.678-5\n16/06/2023"), L.interpretar_texto("16/06/2023")])
        self.assertIsNone(f["vence"])
        self.assertIn("una_sola_fecha", f["avisos"])
        self.assertTrue(f["legible"])
        errores, _ = L.evaluar(f, rut_chofer=MARIA, hoy=HOY)
        self.assertIn("licencia_vence", errores)                  # sin declarar una fecha de las leídas no se registra

    def test_las_cifras_que_el_ocr_confunde(self):
        for leido in ("25647065-5", "25.647.065-5", "25547065-6", "25.547.065-5"):
            with self.subTest(leido=leido):
                self.assertTrue(L.rut_parecido(leido, "25547065-5"))
        for otro in ("25547075-5", "26647066-5", "26647065-5", "25547065-9", "5547065-5", "13375664-7"):
            with self.subTest(otro=otro):
                self.assertFalse(L.rut_parecido(otro, "25547065-5"))

    def test_un_rut_mal_leido_igual_coincide_con_el_declarado_si_se_parece(self):
        l = L.interpretar_texto("N DE LICENCIA 25.647.065-5\nFECHA DE CONTROL 29/10/2029")      # el 5 leído como 6: el verificador no cuadra
        self.assertIsNone(l["rut"])
        self.assertTrue(L.rut_coincide(l, "25.547.065-5"))
        errores, avisos = L.evaluar(l, rut_chofer="25.547.065-5", hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("cifra dudosa" in a for a in avisos))

    def test_el_rut_mal_leido_no_coincide_con_el_de_otra_persona(self):
        l = L.interpretar_texto("N DE LICENCIA 25.647.065-5\nFECHA DE CONTROL 29/10/2029")
        errores, _ = L.evaluar(l, rut_chofer="13.375.664-7", hoy=HOY)
        self.assertIn("rut", errores)
        self.assertIn("leído con dudas", errores["rut"])

    def test_el_usuario_con_otro_rut_se_bloquea_aunque_el_ocr_haya_fallado_una_cifra(self):
        l = L.interpretar_texto("N DE LICENCIA 25.647.065-5\nFECHA DE CONTROL 29/10/2029")
        errores, _ = L.evaluar(l, rut_chofer="25.547.065-5", usuario={"nombre": "Milagros Salazar", "rut": "12000005-5"}, hoy=HOY)
        self.assertIn("usuario_id", errores)
        self.assertNotIn("rut", errores)

    def test_la_fecha_declarada_tiene_que_ser_una_de_las_leidas(self):
        f = L.fusionar([L.interpretar_texto(self.PASADA_FECHA), L.interpretar_texto(self.PASADA_FECHA)])
        self.assertEqual(L.fecha_de_control(f, date(2029, 10, 29)), (date(2029, 10, 29), None))
        self.assertEqual(L.fecha_de_control(f, date(2023, 6, 16)), (date(2023, 6, 16), None))      # otra de las leídas: la persona decide
        fecha, error = L.fecha_de_control(f, date(2031, 1, 1))
        self.assertIsNone(fecha)
        self.assertIn("no aparece en la licencia escaneada", error)
        self.assertIn("29/10/2029", error)

    def test_sin_declarar_vale_la_que_se_reconocio(self):
        f = L.fusionar([L.interpretar_texto(self.PASADA_FECHA)] * 2)
        self.assertEqual(L.fecha_de_control(f), (date(2029, 10, 29), None))

    def test_evaluar_exige_que_la_fecha_declarada_este_en_la_foto_y_no_este_vencida(self):
        f = L.fusionar([L.interpretar_texto(self.PASADA_FECHA)] * 2)
        errores, _ = L.evaluar(f, rut_chofer=None, vence_declarado=date(2031, 1, 1), hoy=HOY)
        self.assertIn("licencia_vence", errores)
        errores, _ = L.evaluar(f, vence_declarado=date(2023, 6, 16), hoy=HOY)               # leída, pero ya pasó
        self.assertIn("venció", errores["licencia_vence"])
        errores, _ = L.evaluar(f, vence_declarado=date(2029, 10, 29), hoy=HOY)
        self.assertEqual(errores, {})


class TestRevisionDeSeguridad(unittest.TestCase):
    def test_si_se_leyo_un_rut_valido_manda_ese_y_no_otro_que_se_le_parezca(self):
        otro = L.interpretar_texto("N DE LICENCIA 12.345.678-5\nFECHA DE CONTROL 29/10/2029\n12.345.679-3")
        self.assertEqual(otro["rut"], "12345678-5")
        self.assertFalse(L.rut_coincide(otro, "12.345.679-3"))          # el RUT de OTRA persona que aparece al lado no sirve

    def test_la_tolerancia_de_una_cifra_solo_vale_cuando_no_se_leyo_ningun_rut_valido(self):
        l = L.interpretar_texto("N DE LICENCIA 25.647.065-5\nFECHA DE CONTROL 29/10/2029")
        self.assertIsNone(l["rut"])
        self.assertTrue(L.rut_coincide(l, "25.547.065-5"))

    def test_dos_rut_validos_distintos_leidos_por_varias_pasadas_es_un_error(self):
        dos = "N DE LICENCIA 12.345.678-5\nFECHA DE CONTROL 29/10/2029\n" + R.rut_formato(PEDRO)
        f = L.fusionar([L.interpretar_texto(dos), L.interpretar_texto(dos)])
        self.assertIn("varios_rut_fuerte", f["avisos"])
        errores, _ = L.evaluar(f, rut_chofer=MARIA, hoy=HOY)
        self.assertIn("licencia_archivo", errores)
        self.assertIn("más de un RUT", errores["licencia_archivo"])

    def test_un_rut_ajeno_leido_por_una_sola_pasada_no_bloquea(self):
        base = "N DE LICENCIA 12.345.678-5\nFECHA DE CONTROL 29/10/2029"
        f = L.fusionar([L.interpretar_texto(base), L.interpretar_texto(base), L.interpretar_texto(base + "\n" + R.rut_formato(PEDRO))])
        self.assertNotIn("varios_rut_fuerte", f["avisos"])

    def test_sin_rut_en_el_perfil_el_nombre_pide_tres_palabras(self):
        l = L.interpretar_texto(LIMPIO)
        errores, _ = L.evaluar(l, rut_chofer=MARIA, usuario={"nombre": "María Pérez", "rut": ""}, hoy=HOY)       # solo 2 palabras en común
        self.assertIn("usuario_id", errores)
        errores, avisos = L.evaluar(l, rut_chofer=MARIA, usuario={"nombre": "María Elena Pérez", "rut": ""}, hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("sin RUT" in a or "no tiene RUT" in a for a in avisos))

    def test_el_mensaje_de_otro_usuario_no_repite_el_rut_de_su_perfil(self):
        l = L.interpretar_texto(LIMPIO)
        errores, _ = L.evaluar(l, rut_chofer=MARIA, usuario={"nombre": "Milagros Salazar", "rut": PEDRO}, hoy=HOY)
        self.assertNotIn(R.rut_formato(PEDRO), errores["usuario_id"])
        self.assertNotIn(PEDRO, errores["usuario_id"])
        self.assertIn("Milagros Salazar", errores["usuario_id"])

    def test_una_fecha_de_control_a_mas_de_siete_anos_se_rechaza(self):
        l = L.interpretar_texto(LIMPIO.replace("29/10/2029", "29/10/2040"))
        errores, _ = L.evaluar(l, rut_chofer=MARIA, hoy=HOY)
        self.assertIn("demasiado lejana", errores["licencia_vence"])
        l = L.interpretar_texto(LIMPIO.replace("29/10/2029", "29/10/2032"))
        errores, _ = L.evaluar(l, rut_chofer=MARIA, hoy=HOY)
        self.assertEqual(errores, {})


class TestJsonParaElNavegador(unittest.TestCase):
    def test_las_fechas_leidas_viajan_para_que_la_persona_elija(self):
        f = L.fusionar([L.interpretar_texto(LIMPIO)] * 2)
        j = L.lectura_a_json(f)
        self.assertEqual([x["iso"] for x in j["fechas"]], ["2023-06-16", "2029-10-29"])
        self.assertEqual([x["fmt"] for x in j["fechas"]], [R.fecha_corta(date(2023, 6, 16)), R.fecha_corta(date(2029, 10, 29))])
        self.assertEqual(j["ruts_leidos"], ["12345678-5"])

    def test_las_palabras_viajan_para_poder_comparar_sin_repetir_el_ocr(self):
        j = L.lectura_a_json(L.interpretar_texto(LIMPIO))
        self.assertIn("MARIA", j["palabras"])

    def test_las_fechas_van_en_iso_y_en_texto_de_persona(self):
        j = L.lectura_a_json(L.interpretar_texto(LIMPIO))
        self.assertEqual(j["vence"], "2029-10-29")
        self.assertEqual(j["vence_fmt"], R.fecha_corta(date(2029, 10, 29)))
        self.assertEqual(j["rut_formato"], "12.345.678-5")
        self.assertEqual(j["clases"], ["B"])
        self.assertTrue(j["legible"])

    def test_los_avisos_traen_su_explicacion(self):
        j = L.lectura_a_json(L.interpretar_texto("CLASE B"))
        textos = {a["codigo"]: a["texto"] for a in j["avisos"]}
        self.assertIn("sin_rut", textos)
        self.assertIn("RUT", textos["sin_rut"])

    def test_sirve_con_una_lectura_vacia(self):
        j = L.lectura_a_json(None)
        self.assertFalse(j["legible"])
        self.assertIsNone(j["vence"])


class TestCompararConElRegistro(unittest.TestCase):
    def setUp(self):
        self.l = L.interpretar_texto(LIMPIO)

    def ev(self, **k):
        k.setdefault("hoy", HOY)
        return L.evaluar(self.l, **k)

    def test_todo_coincide(self):
        errores, avisos = self.ev(rut_chofer=MARIA, nombre_chofer="María Elena Pérez Soto", usuario={"nombre": "María Pérez", "rut": MARIA},
                                  clases_declaradas=["B"])
        self.assertEqual(errores, {})
        self.assertEqual(avisos, [])                       # una lectura de UN solo texto (interpretar_texto) no avisa de «fecha poco segura»: solo fusionar lo hace

    def test_el_rut_de_la_licencia_no_es_el_del_chofer(self):
        errores, _ = self.ev(rut_chofer=PEDRO)
        self.assertIn("rut", errores)
        self.assertIn("12.345.678-5", errores["rut"])

    def test_el_usuario_elegido_tiene_otro_rut_es_el_caso_de_milagros(self):
        # Se registra a una persona (RUT de la licencia) colgándola del usuario de OTRA (Milagros): se bloquea.
        errores, _ = self.ev(rut_chofer=MARIA, usuario={"nombre": "Milagros Salazar", "rut": PEDRO})
        self.assertIn("usuario_id", errores)
        self.assertIn("propio usuario", errores["usuario_id"])
        self.assertNotIn("rut", errores)

    def test_usuario_sin_rut_en_el_perfil_y_nombre_distinto_se_bloquea(self):
        errores, _ = self.ev(rut_chofer=MARIA, usuario={"nombre": "Milagros Salazar", "rut": ""})
        self.assertIn("usuario_id", errores)
        self.assertIn("Mi cuenta", errores["usuario_id"])

    def test_usuario_sin_rut_pero_con_el_mismo_nombre_pasa_con_aviso(self):
        errores, avisos = self.ev(rut_chofer=MARIA, usuario={"nombre": "María Elena Pérez", "rut": None})
        self.assertEqual(errores, {})
        self.assertTrue(any("sin RUT" in a or "no tiene RUT" in a for a in avisos))

    def test_el_nombre_se_compara_sin_acentos_ni_orden(self):
        self.assertTrue(L.nombres_parecidos("PEREZ SOTO MARIA ELENA", "María Elena Pérez"))
        self.assertTrue(L.nombres_parecidos("MARIA PEREZ", "Maria Pérez Soto"))
        self.assertFalse(L.nombres_parecidos("MARIA PEREZ", "Milagros Salazar"))
        self.assertFalse(L.nombres_parecidos("", "Milagros Salazar"))
        self.assertFalse(L.nombres_parecidos("Del Valle", "Valle de Luna Pérez"))        # una sola palabra en común no basta
        self.assertFalse(L.nombres_parecidos("Milagros", "Milagros Salazar"))           # un nombre de una sola palabra nunca se da por comprobado

    def test_licencia_vencida_se_bloquea(self):
        l = L.interpretar_texto(LIMPIO.replace("29/10/2029", "29/10/2025"))
        errores, _ = L.evaluar(l, rut_chofer=MARIA, hoy=HOY)
        self.assertIn("licencia_vence", errores)
        self.assertIn("venció", errores["licencia_vence"])

    def test_licencia_por_vencer_avisa_pero_deja(self):
        l = L.interpretar_texto(LIMPIO.replace("29/10/2029", "20/11/2026"))
        errores, avisos = L.evaluar(l, rut_chofer=MARIA, hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("vence en 15 días" in a for a in avisos))

    def test_clase_que_la_licencia_no_trae(self):
        errores, _ = self.ev(rut_chofer=MARIA, clases_declaradas=["A4", "B"])
        self.assertIn("licencia_clases", errores)
        self.assertIn("A4", errores["licencia_clases"])

    def test_clase_marcada_dentro_de_las_que_trae(self):
        l = L.interpretar_texto(LIMPIO.replace("CLASE B", "CLASE A4 B"))
        errores, _ = L.evaluar(l, rut_chofer=MARIA, clases_declaradas=["B"], hoy=HOY)
        self.assertEqual(errores, {})

    def test_si_no_se_leyo_la_clase_se_avisa_y_no_se_bloquea(self):
        l = L.interpretar_texto(LIMPIO.replace("CLASE B\n", ""))
        errores, avisos = L.evaluar(l, rut_chofer=MARIA, clases_declaradas=["B"], hoy=HOY)
        self.assertEqual(errores, {})
        self.assertTrue(any("clase" in a.lower() for a in avisos))

    def test_nombre_escrito_distinto_al_de_la_licencia_solo_avisa(self):
        errores, avisos = self.ev(rut_chofer=MARIA, nombre_chofer="Juan Pérez González")
        self.assertEqual(errores, {})
        self.assertTrue(any("no se parece" in a for a in avisos))

    def test_licencia_ilegible_no_deja_comparar_nada(self):
        errores, _ = L.evaluar(L.interpretar_texto("borroso"), rut_chofer=MARIA, hoy=HOY)
        self.assertEqual(list(errores), ["licencia_archivo"])
        self.assertIn("No pude leer la licencia", errores["licencia_archivo"])

    def test_el_mensaje_de_ilegible_dice_que_falto(self):
        errores, _ = L.evaluar(L.interpretar_texto("CLASE B"), hoy=HOY)
        self.assertIn("RUT", errores["licencia_archivo"])
        self.assertIn("fecha de control", errores["licencia_archivo"])


if __name__ == "__main__":
    unittest.main()

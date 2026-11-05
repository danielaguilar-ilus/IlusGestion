# -*- coding: utf-8 -*-
"""🚚 FLOTA · motor del escáner de licencias (imagen → texto → datos). El OCR de verdad es Tesseract; aquí se prueba TODO lo que lo rodea con un
OCR de mentira (orientación del celular, PDF, tamaño, varios filtros, corte por tiempo, motor ausente) y, si Tesseract está instalado en esta
máquina, también la lectura real de una licencia dibujada.

Correr:  python -m pytest tests/test_flota_licencia_ocr.py -q -p no:warnings
"""
import io
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_licencia_ocr as O  # noqa: E402
from test_flota_licencia_lector import LIMPIO  # noqa: E402

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:                                         # pragma: no cover
    Image = None


def png(ancho=1600, alto=1000, color=(244, 196, 150)):
    img = Image.new("RGB", (ancho, alto), color)
    b = io.BytesIO()
    img.save(b, "PNG")
    return b.getvalue()


def jpeg_con_orientacion(ancho, alto, orientacion):
    img = Image.new("RGB", (ancho, alto), (240, 190, 140))
    exif = Image.Exif()
    exif[0x0112] = orientacion
    b = io.BytesIO()
    img.save(b, "JPEG", exif=exif)
    return b.getvalue()


def pdf_de(img):
    b = io.BytesIO()
    img.save(b, "PDF")
    return b.getvalue()


class OcrFalso:
    """Devuelve textos en orden y anota con qué lo llamaron."""

    def __init__(self, *textos, fn=None):
        self.textos, self.fn, self.llamadas = list(textos), fn, []

    def __call__(self, imagen, psm, extra=""):
        self.llamadas.append((imagen.size, imagen.mode, psm, extra))
        if self.fn:
            return self.fn(imagen, psm)
        return self.textos[min(len(self.llamadas) - 1, len(self.textos) - 1)]


@unittest.skipIf(Image is None, "sin Pillow")
class TestAbrirLaImagen(unittest.TestCase):
    def test_una_foto_png_se_abre_en_rgb(self):
        img = O.abrir_imagen(png(800, 500))
        self.assertEqual(img.mode, "RGB")
        self.assertEqual(img.size, (800, 500))

    def test_una_foto_chica_se_agranda_y_una_enorme_se_achica(self):
        self.assertEqual(max(O.normalizar_tamano(O.abrir_imagen(png(800, 500))).size), O.LADO_LARGO)
        self.assertEqual(max(O.normalizar_tamano(O.abrir_imagen(png(4000, 2500))).size), O.LADO_LARGO)

    def test_una_foto_de_tamano_normal_queda_como_vino(self):
        img = O.normalizar_tamano(O.abrir_imagen(png(1950, 1200)))
        self.assertEqual(img.size, (1950, 1200))

    def test_el_margen_blanco_de_una_pagina_se_recorta(self):
        # Una hoja (PDF) con la tarjeta en el medio: Tesseract no leía NADA de la hoja entera (licencia real, 05-nov-2026).
        hoja = Image.new("RGB", (1500, 2100), (255, 255, 255))
        hoja.paste(Image.new("RGB", (1500, 900), (240, 190, 140)), (0, 600))
        rec = O.recortar_papel(hoja)
        self.assertLess(rec.size[1], 1000)
        self.assertGreater(rec.size[1], 880)
        self.assertEqual(rec.size[0], 1500)

    def test_una_foto_normal_no_se_recorta(self):
        foto = Image.new("RGB", (1600, 1000), (200, 160, 120))
        self.assertEqual(O.recortar_papel(foto).size, (1600, 1000))
        con_esquina = foto.copy()
        con_esquina.paste(Image.new("RGB", (200, 150), (255, 255, 255)), (0, 0))
        self.assertEqual(O.recortar_papel(con_esquina).size, (1600, 1000))

    def test_una_hoja_en_blanco_no_rompe_el_recorte(self):
        self.assertEqual(O.recortar_papel(Image.new("RGB", (800, 600), (255, 255, 255))).size, (800, 600))

    def test_la_orientacion_del_celular_se_corrige(self):
        # Orientación EXIF 6 = el celular la sacó acostado: 1800×1100 en el archivo, pero se ve de 1100×1800.
        img = O.abrir_imagen(jpeg_con_orientacion(1800, 1100, 6))
        self.assertGreater(img.size[1], img.size[0])

    def test_un_pdf_se_lee_por_su_primera_pagina(self):
        pagina = Image.new("RGB", (900, 600), (255, 255, 255))
        img = O.abrir_imagen(pdf_de(pagina))
        self.assertEqual(img.mode, "RGB")
        self.assertGreater(max(img.size), 1000)

    def test_lo_que_no_es_foto_ni_pdf_se_rechaza(self):
        for basura in (b"", b"hola mundo", b"\x00" * 200, b"GIF89a" + b"\x00" * 50):
            with self.subTest(basura=basura[:8]):
                with self.assertRaises(O.ArchivoIlegible):
                    O.abrir_imagen(basura)

    def test_una_foto_cortada_o_rota_se_rechaza(self):
        with self.assertRaises(O.ArchivoIlegible):
            O.abrir_imagen(png(1600, 1000)[:200])

    def test_una_imagen_gigante_se_rechaza_antes_de_cargarla(self):
        viejo = O.MAX_PIXELES
        O.MAX_PIXELES = 100_000
        try:
            with self.assertRaises(O.ArchivoIlegible):
                O.abrir_imagen(png(1600, 1000))
        finally:
            O.MAX_PIXELES = viejo

    def test_las_variantes_son_tres_filtros_de_la_misma_foto(self):
        v = O.variantes(O.abrir_imagen(png(1800, 1100)))
        self.assertEqual([n for n, _ in v], ["rojo", "gris", "verde"])
        self.assertTrue(all(i.size == (1800, 1100) and i.mode == "L" for _, i in v))


@unittest.skipIf(Image is None, "sin Pillow")
class TestLeerLaLicencia(unittest.TestCase):
    def test_lectura_completa_y_se_corta_a_las_dos_pasadas(self):
        ocr = OcrFalso(LIMPIO)
        lectura, meta = O.leer_licencia(png(), ocr=ocr)
        self.assertTrue(lectura["legible"])
        self.assertEqual(lectura["rut"], "12345678-5")
        self.assertEqual(lectura["vence"], date(2029, 10, 29))
        self.assertEqual(len(ocr.llamadas), 2)                    # no sigue gastando CPU cuando ya está completa
        self.assertEqual(meta["pasadas"], 2)
        self.assertEqual(meta["motor"], "inyectado")

    def test_una_pasada_que_lee_mal_se_corrige_con_las_otras(self):
        mala = LIMPIO.replace("29/10/2029", "28/10/2029")
        ocr = OcrFalso(mala, LIMPIO, LIMPIO, LIMPIO)
        lectura, _ = O.leer_licencia(png(), ocr=ocr)
        self.assertEqual(lectura["vence"], date(2029, 10, 29))

    def test_cada_pasada_recibe_un_filtro_y_un_modo_distinto_incluido_solo_digitos(self):
        ocr = OcrFalso("nada legible")
        O.leer_licencia(png(1800, 1100), ocr=ocr)
        psms = [c[2] for c in ocr.llamadas]
        self.assertNotIn(5, psms)
        self.assertIn(11, psms)
        self.assertTrue(all(c[1] == "L" for c in ocr.llamadas[:len(O.PASADAS)]))
        extras = [c[3] for c in ocr.llamadas[:len(O.PASADAS)]]
        self.assertIn(O.SOLO_DIGITOS, extras)                      # las pasadas de solo dígitos aciertan mucho más el RUT y las fechas
        self.assertIn("", extras)
        self.assertEqual(len(ocr.llamadas), len(O.PASADAS) + 3)    # nada legible: todas las pasadas y las tres vueltas

    def test_si_no_se_lee_nada_devuelve_una_lectura_no_legible_y_no_lanza(self):
        lectura, meta = O.leer_licencia(png(), ocr=OcrFalso("borroso borroso"))
        self.assertFalse(lectura["legible"])
        self.assertGreaterEqual(meta["pasadas"], len(O.PASADAS))

    def test_una_foto_de_lado_se_prueba_girada(self):
        # El OCR de mentira solo entiende la foto cuando está derecha (más alta que ancha): la lectura necesita el giro.
        ocr = OcrFalso(fn=lambda im, psm: LIMPIO if im.size[1] > im.size[0] else "ruido")
        lectura, meta = O.leer_licencia(png(1800, 1100), ocr=ocr)
        self.assertTrue(lectura["legible"])
        self.assertTrue(any("90" in f or "270" in f for f in meta["filtros"]))

    def test_corta_por_tiempo(self):
        reloj_t = [0.0]

        def reloj():
            reloj_t[0] += 9.0                                      # cada consulta al reloj «pasan» 9 s
            return reloj_t[0]
        ocr = OcrFalso("borroso")
        lectura, meta = O.leer_licencia(png(), ocr=ocr, reloj=reloj)
        self.assertFalse(lectura["legible"])
        self.assertLess(len(ocr.llamadas), 6)

    def test_si_el_motor_no_esta_se_dice_y_no_se_inventa_una_lectura(self):
        def sin_motor(imagen, psm, extra=""):
            raise O.MotorNoDisponible("Tesseract no está instalado.")
        with self.assertRaises(O.MotorNoDisponible):
            O.leer_licencia(png(), ocr=sin_motor)

    def test_un_archivo_que_no_es_imagen_lanza_ilegible_antes_de_gastar_OCR(self):
        ocr = OcrFalso(LIMPIO)
        with self.assertRaises(O.ArchivoIlegible):
            O.leer_licencia(b"no soy una foto", ocr=ocr)
        self.assertEqual(ocr.llamadas, [])

    def test_la_hoja_con_la_tarjeta_en_el_medio_le_llega_recortada_al_ocr(self):
        hoja = Image.new("RGB", (1500, 2100), (255, 255, 255))
        hoja.paste(Image.new("RGB", (1500, 900), (240, 190, 140)), (0, 600))
        ocr = OcrFalso(LIMPIO)
        O.leer_licencia(pdf_de(hoja), ocr=ocr)
        ancho, alto = ocr.llamadas[0][0]
        self.assertGreater(ancho, alto)                            # ya no es una hoja vertical: es la tarjeta

    def test_dos_pasadas_que_coinciden_en_rut_y_fecha_cortan_la_lectura(self):
        ocr = OcrFalso(LIMPIO, LIMPIO)
        _l, meta = O.leer_licencia(png(), ocr=ocr)
        self.assertEqual(meta["pasadas"], 2)

    def test_un_rut_mal_leido_no_corta_la_lectura_se_siguen_probando_pasadas(self):
        # «25.647.065-5» (un 5 leído como 6) no tiene dígito verificador válido: ninguna pasada lo confirma, así que se prueban todas
        mal = "N DE LICENCIA 25.647.065-5\nFECHA DE CONTROL 29/10/2029"
        ocr = OcrFalso(mal)
        lectura, meta = O.leer_licencia(png(), ocr=ocr)
        self.assertEqual(meta["pasadas"], len(O.PASADAS))
        self.assertIsNone(lectura["rut"])
        self.assertEqual(lectura["ruts_leidos"], ["25647065-5"])
        self.assertTrue(lectura["legible"])                         # sirve para comparar con el RUT que declara la persona

    def test_basta_un_rut_valido_y_dos_pasadas_con_la_misma_fecha(self):
        ocr = OcrFalso("N DE LICENCIA 12.345.678-5", "FECHA DE CONTROL 29/10/2029", "FECHA DE CONTROL 29/10/2029", "x", "x", "x", "x", "x")
        _l, meta = O.leer_licencia(png(), ocr=ocr)
        self.assertEqual(meta["pasadas"], 3)

    def test_el_rut_y_la_fecha_pueden_confirmarse_en_pasadas_distintas(self):
        solo_rut = "12.345.678-5"
        solo_fecha = "FECHA DE CONTROL 29/10/2029"
        ocr = OcrFalso(solo_rut, solo_fecha, solo_rut, solo_fecha, "x", "x", "x", "x")
        lectura, meta = O.leer_licencia(png(), ocr=ocr)
        self.assertTrue(lectura["legible"])
        self.assertEqual(meta["pasadas"], 4)                       # RUT leído 2 veces y fecha leída 2 veces: ya alcanza

    def test_funciona_con_un_pdf(self):
        pagina = Image.new("RGB", (1000, 640), (250, 200, 150))
        lectura, _ = O.leer_licencia(pdf_de(pagina), ocr=OcrFalso(LIMPIO))
        self.assertTrue(lectura["legible"])


def pdf_con_pagina(ancho, alto):
    """Un PDF mínimo (menos de 1 KB) que DECLARA una página del tamaño que se pida: así se prueba el ataque de la página gigante."""
    cabeza = b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    pagina = ("3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 %d %d]>>endobj\ntrailer<</Root 1 0 R>>\n" % (ancho, alto)).encode("ascii")
    return cabeza + pagina + b"%%EOF"


@unittest.skipIf(Image is None, "sin Pillow")
class TestRevisionDeSeguridad(unittest.TestCase):
    def test_un_pdf_diminuto_con_una_pagina_gigante_nunca_se_dibuja_a_su_tamano_real(self):
        # 20.000 × 20.000 pt dibujados a escala 2,5 serían 50.000 × 50.000 px (10 GB): la escala se baja ANTES de dibujar.
        pdf = pdf_con_pagina(20000, 20000)
        self.assertLess(len(pdf), 1024)
        img = O.abrir_imagen(pdf)
        self.assertLessEqual(max(img.size), 3001)
        self.assertLessEqual(img.size[0] * img.size[1], 9_100_000)

    def test_la_escala_solo_se_baja_cuando_hace_falta(self):
        import inspect
        fuente = inspect.getsource(O.abrir_imagen)
        self.assertIn("escala = min(2.5, 3000.0 / max(ancho, alto))", fuente)
        self.assertIn("ancho * escala * alto * escala > MAX_PIXELES", fuente)           # y si aun así pasara del tope, se rechaza

    def test_una_hoja_normal_se_sigue_leyendo(self):
        img = O.abrir_imagen(pdf_con_pagina(595, 842))
        self.assertGreater(img.size[0], 1000)

    def test_una_pagina_enorme_pero_dentro_del_tope_se_reduce_en_vez_de_rechazarse(self):
        img = O.abrir_imagen(pdf_con_pagina(3000, 4000))
        self.assertLessEqual(img.size[0] * img.size[1], O.MAX_PIXELES)

    def test_el_tope_de_pixeles_de_una_foto_es_bajo(self):
        self.assertLessEqual(O.MAX_PIXELES, 25_000_000)

    def test_solo_se_abren_jpeg_png_y_webp(self):
        import inspect
        self.assertIn('formats=("JPEG", "PNG", "WEBP")', inspect.getsource(O.abrir_imagen))

    def test_pdfium_se_usa_de_a_uno(self):
        import inspect
        self.assertIn("with _PDFIUM:", inspect.getsource(O.abrir_imagen))
        self.assertIn("pdf.close()", inspect.getsource(O.abrir_imagen))


class TestElMotorDisponible(unittest.TestCase):
    def test_no_lanza_y_devuelve_el_estado(self):
        ok, info = O.motor_disponible()
        self.assertIsInstance(ok, bool)
        self.assertTrue(info)

    def test_sin_pytesseract_el_ocr_real_dice_motor_no_disponible(self):
        import builtins
        real = builtins.__import__

        def sin_pytesseract(nombre, *a, **k):
            if nombre == "pytesseract":
                raise ImportError("no hay")
            return real(nombre, *a, **k)
        builtins.__import__ = sin_pytesseract
        try:
            with self.assertRaises(O.MotorNoDisponible):
                O._ocr_tesseract(Image.new("L", (10, 10)), 6)
            self.assertEqual(O.motor_disponible()[0], False)
        finally:
            builtins.__import__ = real


@unittest.skipUnless(Image is not None and O.motor_disponible()[0], "Tesseract no está instalado en esta máquina: se prueba en producción/Docker")
class TestConTesseractDeVerdad(unittest.TestCase):
    """Dibuja una licencia (fondo anaranjado, letra negra, etiquetas y valores) y la lee con el motor real."""

    def licencia(self, texto, ancho=1800, alto=1100):
        img = Image.new("RGB", (ancho, alto), (246, 200, 155))
        d = ImageDraw.Draw(img)
        fuente = ImageFont.load_default(size=58)
        y = 90
        for ln in texto.strip().split("\n"):
            d.text((90, y), ln, fill=(20, 20, 20), font=fuente)
            y += 84
        b = io.BytesIO()
        img.save(b, "JPEG", quality=88)
        return b.getvalue()

    def test_lee_rut_fecha_clase_y_nombre(self):
        lectura, meta = O.leer_licencia(self.licencia(LIMPIO.replace("Nº", "N")))
        self.assertTrue(lectura["legible"], lectura)
        self.assertEqual(lectura["rut"], "12345678-5")
        self.assertEqual(lectura["vence"], date(2029, 10, 29))
        self.assertEqual(lectura["clases"], ["B"])


if __name__ == "__main__":
    unittest.main()

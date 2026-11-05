# -*- coding: utf-8 -*-
"""🚚 FLOTA · MOTOR DEL ESCÁNER DE LICENCIAS: de una foto (o un PDF) al texto, y de ahí a los datos (flota_licencia_lector).

Daniel, 05-nov-2026: eligió el OCR PROPIO (Tesseract, en el servidor de ILUS): la foto de la licencia —con el RUT y el nombre de la persona—
NO sale de ILUS ni se manda a ningún tercero. Tesseract ya viene en la imagen de producción (Dockerfile: tesseract-ocr + tesseract-ocr-spa) y
pytesseract en requirements.txt: no hay nada nuevo que instalar en el servidor.

Cómo se lee (lo aprendido leyendo una licencia REAL con Tesseract el 05-nov-2026):
  · Tesseract NO lee nada de una página entera de PDF donde el carnet ocupa un tercio y el resto es papel blanco: primero se RECORTA el papel y
    se lleva la tarjeta a un tamaño que se lee bien.
  · Una sola pasada se equivoca en una cifra (un 5 leído como 6, un 1 como 4). Por eso se hacen VARIAS pasadas de la misma foto, con filtros
    distintos y dos modos —texto suelto y «solo dígitos»— y se juntan las lecturas por VOTACIÓN (flota_licencia_lector.fusionar). Un RUT cuyo
    dígito verificador no cuadra se descarta solo: una cifra mal leída nunca da por buena a otra persona.
  · El filtro del canal ROJO ayuda en la licencia chilena (fondo anaranjado, letra negra); el verde y el gris cubren cuando no.
  · Se corta apenas dos pasadas coinciden en RUT y fecha, o cuando se acaba el tiempo (TIEMPO_MAX_S): un escáner lento no sirve en el celular.
Nada se guarda aquí: leer no escribe en ninguna parte.
"""
import io
import threading
import time

import flota_licencia_lector as lector
import flota_reglas as fr

TIEMPO_MAX_S = 28            # tope total de la lectura (varias pasadas del OCR)
TIEMPO_PASADA_S = 14         # tope de UNA pasada de Tesseract
LADO_LARGO = 2000            # a este largo (px) se lleva la tarjeta para leerla
MAX_PIXELES = 25_000_000     # una imagen más grande se rechaza (protege la memoria del servidor: 25 MP ya son ~75 MB por copia)
_PDFIUM = threading.Lock()   # PDFium no es seguro con varios hilos a la vez: se usa de a uno
SOLO_DIGITOS = "-c tessedit_char_whitelist=0123456789./-K"
# (lado largo en px, filtro, modo de Tesseract, configuración extra). 11 = texto suelto: lee mejor la tarjeta que 6 = un bloque, porque las
# etiquetas están desordenadas. «Solo dígitos» acierta mucho más el RUT y las fechas. Dos tamaños: en la licencia real una cifra salía bien a
# 1500 px y mal a 2000, y otra al revés. Van intercaladas para que dos pasadas coincidan lo antes posible.
PASADAS = (
    (1500, "gris", 11, ""),
    (2000, "rojo", 11, ""),
    (1500, "gris", 11, SOLO_DIGITOS),
    (2000, "rojo", 11, SOLO_DIGITOS),
    (1500, "verde", 11, ""),
    (2000, "gris", 11, SOLO_DIGITOS),
    (1500, "rojo", 11, ""),
    (1500, "verde", 11, SOLO_DIGITOS),
)


class MotorNoDisponible(RuntimeError):
    """Tesseract no está instalado o no responde."""


class ArchivoIlegible(ValueError):
    """El archivo no se pudo abrir como imagen ni como PDF."""


# ── El motor ────────────────────────────────────────────────────────────────
_IDIOMA = {}


def motor_disponible():
    """(True, 'tesseract 5.3.0') o (False, motivo). No lanza."""
    try:
        import pytesseract
        return True, "tesseract " + str(pytesseract.get_tesseract_version())
    except Exception as e:                      # falta el módulo o el programa: da lo mismo, el escáner no está
        return False, type(e).__name__


def _idioma():
    """'spa' si está instalado (viene en la imagen de producción); si no, el inglés, que sigue leyendo bien números y fechas."""
    if "v" not in _IDIOMA:
        try:
            import pytesseract
            langs = set(pytesseract.get_languages(config=""))
            _IDIOMA["v"] = "spa" if "spa" in langs else ("eng" if "eng" in langs else None)
        except Exception:
            _IDIOMA["v"] = None
    return _IDIOMA["v"]


def _ocr_tesseract(imagen, psm, extra=""):
    try:
        import pytesseract
    except Exception as e:
        raise MotorNoDisponible("Falta pytesseract: " + type(e).__name__)
    idioma = _idioma()
    if idioma is None:
        raise MotorNoDisponible("Tesseract no tiene ningún idioma instalado.")
    # Con «solo dígitos» el idioma da lo mismo: el inglés es el más rápido y no confunde con letras.
    lang = "eng" if (extra and "eng" in _idiomas_instalados()) else idioma
    try:
        return pytesseract.image_to_string(imagen, lang=lang, config=f"--psm {psm} --oem 3 {extra}".strip(), timeout=TIEMPO_PASADA_S)
    except pytesseract.TesseractNotFoundError as e:
        raise MotorNoDisponible("Tesseract no está instalado.") from e
    except RuntimeError:
        return ""                               # se pasó del tiempo de esa pasada: se sigue con las demás


def _idiomas_instalados():
    try:
        import pytesseract
        return set(pytesseract.get_languages(config=""))
    except Exception:
        return set()


# ── La imagen ───────────────────────────────────────────────────────────────
def abrir_imagen(data):
    """PIL.Image RGB y derecha, desde una foto (JPG/PNG/WebP) o la primera página de un PDF. Lanza ArchivoIlegible."""
    from PIL import Image, ImageOps
    det = fr.detectar_archivo(data)
    if det is None:
        raise ArchivoIlegible("No es una foto ni un PDF que pueda leer.")
    try:
        if det[0] == "pdf":
            import pypdfium2 as pdfium
            with _PDFIUM:
                pdf = pdfium.PdfDocument(bytes(data))
                try:
                    if len(pdf) < 1:
                        raise ArchivoIlegible("El PDF no tiene páginas.")
                    pagina = pdf[0]
                    ancho, alto = pagina.get_size()
                    if ancho <= 0 or alto <= 0:
                        raise ArchivoIlegible("La página del PDF no tiene un tamaño válido.")
                    # Un PDF de 1 KB puede declarar una página de 20.000 pt por lado y pedir GB de memoria al dibujarla: se mide ANTES.
                    escala = min(2.5, 3000.0 / max(ancho, alto))
                    if ancho * escala * alto * escala > MAX_PIXELES:
                        raise ArchivoIlegible("La página del PDF es demasiado grande para leerla.")
                    img = pagina.render(scale=escala).to_pil()
                finally:
                    pdf.close()
        else:
            img = Image.open(io.BytesIO(bytes(data)), formats=("JPEG", "PNG", "WEBP"))
            if img.size[0] * img.size[1] > MAX_PIXELES:
                raise ArchivoIlegible("La foto es demasiado grande para leerla.")
            if img.format == "JPEG":
                img.draft("RGB", (2600, 2600))                 # el JPEG se decodifica ya reducido: menos memoria y más rápido
            img.load()
            img = ImageOps.exif_transpose(img)
    except ArchivoIlegible:
        raise
    except Exception as e:
        raise ArchivoIlegible("No pude abrir el archivo: " + type(e).__name__) from e
    if img.size[0] * img.size[1] > MAX_PIXELES:
        raise ArchivoIlegible("La imagen es demasiado grande para leerla.")
    return img.convert("RGB")


def recortar_papel(img, umbral=236, minimo_util=0.92):
    """Quita el margen BLANCO de una página (un PDF o un escaneo con la tarjeta en el medio): Tesseract no lee nada de una hoja donde la
    licencia ocupa un tercio. Si lo que no es papel ya ocupa casi toda la imagen (una foto normal), no toca nada."""
    from PIL import ImageOps
    g = ImageOps.grayscale(img)
    caja = g.point(lambda p: 255 if p < umbral else 0).getbbox()
    if not caja:
        return img
    x0, y0, x1, y1 = caja
    if (x1 - x0) * (y1 - y0) >= minimo_util * img.size[0] * img.size[1]:
        return img
    mx, my = max(8, int((x1 - x0) * 0.02)), max(8, int((y1 - y0) * 0.02))
    return img.crop((max(0, x0 - mx), max(0, y0 - my), min(img.size[0], x1 + mx), min(img.size[1], y1 + my)))


def normalizar_tamano(img, objetivo=None):
    """Lleva el lado largo a `objetivo` (LADO_LARGO): se achica lo enorme y se agranda lo chico (Tesseract lee mal las letras diminutas)."""
    from PIL import Image
    objetivo = objetivo or LADO_LARGO
    largo = max(img.size)
    if largo <= 0 or abs(largo - objetivo) < 0.06 * objetivo:
        return img
    f = objetivo / float(largo)
    return img.resize((max(1, int(img.size[0] * f)), max(1, int(img.size[1] * f))), Image.LANCZOS)


def filtros(img):
    """{nombre: imagen en gris} de la MISMA foto con distintos filtros (se calculan al pedirlos para no gastar CPU de más)."""
    from PIL import ImageOps

    class _Filtros(dict):
        def __missing__(self, k):
            if k == "rojo":
                v = ImageOps.autocontrast(img.split()[0], cutoff=2)
            elif k == "verde":
                v = ImageOps.autocontrast(img.split()[1], cutoff=2)
            elif k == "gris":
                v = ImageOps.autocontrast(ImageOps.grayscale(img), cutoff=2)
            else:
                raise KeyError(k)
            self[k] = v
            return v
    return _Filtros()


def variantes(img):
    """[(nombre, imagen)] de la MISMA foto con distintos filtros (la usan las pruebas y el giro de una foto de lado)."""
    f = filtros(img)
    return [("rojo", f["rojo"]), ("gris", f["gris"]), ("verde", f["verde"])]


def _completa(l):
    return bool(l.get("legible") and l.get("clases") and l.get("nombre"))


def _confirmada(lecturas, fusion):
    """La lectura está lista para cortar cuando hay un RUT que EXISTE (su dígito verificador cuadra: eso ya lo confirma) y una fecha de control que
    por lo menos DOS pasadas distintas leyeron igual (una fecha sola podría ser una cifra mal leída y no tiene dígito verificador).
    Un RUT mal leído («25.647.065-5») no alcanza para cortar: se siguen probando pasadas hasta que alguna lo lea bien o se acabe el tiempo."""
    if not fusion.get("rut") or not fusion.get("vence"):
        return False
    return sum(1 for l in lecturas if l.get("vence") == fusion["vence"]) >= 2


# ── La lectura ──────────────────────────────────────────────────────────────
def leer_licencia(data, ocr=None, reloj=time.monotonic):
    """Lee la licencia de `data` (bytes de una foto o un PDF). Devuelve (lectura, meta):
    lectura = lo que arma flota_licencia_lector (rut, vence, clases, nombre, avisos, legible…);
    meta = {"pasadas": n, "filtros": [...], "ms": n, "motor": "tesseract 5.x"}.
    `ocr(imagen, psm, extra) -> texto` se puede inyectar (pruebas). Lanza MotorNoDisponible o ArchivoIlegible; una foto borrosa NO lanza:
    devuelve una lectura no `legible`."""
    t0 = reloj()
    ocr = ocr or _ocr_tesseract
    base = recortar_papel(abrir_imagen(data))
    por_tamano = {}

    def filtros_a(ancho):
        if ancho not in por_tamano:
            por_tamano[ancho] = filtros(normalizar_tamano(base, ancho))
        return por_tamano[ancho]
    lecturas, usados = [], []
    fusion = lector.fusionar([])

    def pasada(nombre, imagen, psm, extra):
        nonlocal fusion
        texto = ocr(imagen, psm, extra)
        lecturas.append(lector.interpretar_texto(texto))
        usados.append(f"{nombre}/psm{psm}" + ("/dig" if extra else ""))
        fusion = lector.fusionar(lecturas)

    for ancho, filtro, psm, extra in PASADAS:
        if reloj() - t0 > TIEMPO_MAX_S or _confirmada(lecturas, fusion):
            break
        pasada(f"{filtro}{ancho}", filtros_a(ancho)[filtro], psm, extra)
    if not fusion["legible"] and reloj() - t0 <= TIEMPO_MAX_S:
        # Foto de lado o boca abajo (EXIF perdido, p. ej. una captura de pantalla): se prueba girada.
        for grados in (90, 270, 180):
            if reloj() - t0 > TIEMPO_MAX_S:
                break
            girada = filtros(normalizar_tamano(base.rotate(grados, expand=True), 1500))
            pasada(f"gris{grados}", girada["gris"], 11, "")
            if fusion["legible"]:
                pasada(f"rojo{grados}", girada["rojo"], 11, SOLO_DIGITOS)
                break
    ok, motor = motor_disponible() if ocr is _ocr_tesseract else (True, "inyectado")
    return fusion, {"pasadas": len(lecturas), "filtros": usados, "ms": int((reloj() - t0) * 1000), "motor": motor if ok else None}

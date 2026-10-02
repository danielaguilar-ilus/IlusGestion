"""REGLA #4.4 — CheckWMS es SOLO LECTURA (Daniel 2026-10-02: "en Check no se
altera nada, siempre siempre siempre solo se consulta").

Se revisa el código fuente como texto (ast.parse de app.py tarda más de un
minuto: tiene ~65 mil líneas):
  1. _checkwms_get es la única puerta a CheckWMS y solo usa requests.get.
  2. Rechaza cualquier ruta fuera de _CHECKWMS_GET_PERMITIDOS antes de salir
     a la red, y esa lista solo trae reportes GET (/api/ext/Get...).
  3. Ningún otro archivo .py le habla a la API de Check directo.
"""
import os
import re
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP = os.path.join(RAIZ, "app.py")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as f:
        return f.read()


def _funcion(src, nombre):
    """Texto de una función de primer nivel: desde su `def` hasta el próximo
    def/decorador/clase en la columna 0."""
    i = src.index(f"\ndef {nombre}(")
    m = re.compile(r"\n(?:def |class |@)").search(src, i + 1)
    return src[i:m.start() if m else len(src)]


class TestCheckwmsSoloLectura(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _leer(APP)
        cls.puerta = _funcion(cls.src, "_checkwms_get")

    def test_lista_blanca_solo_reportes_get(self):
        m = re.search(r"_CHECKWMS_GET_PERMITIDOS\s*=\s*frozenset\(\{(.*?)\}\)", self.src, re.S)
        self.assertTrue(m, "falta _CHECKWMS_GET_PERMITIDOS")
        rutas = re.findall(r'"([^"]+)"', m.group(1))
        self.assertTrue(rutas)
        for ruta in rutas:
            self.assertTrue(ruta.startswith("/api/ext/Get"), f"ruta no es un reporte GET: {ruta}")

    def test_puerta_unica_solo_get_y_rechaza_fuera_de_lista(self):
        self.assertIn("path not in _CHECKWMS_GET_PERMITIDOS", self.puerta)
        # el chequeo de la lista va ANTES de la llamada de red
        self.assertLess(self.puerta.index("_CHECKWMS_GET_PERMITIDOS"), self.puerta.index("_req.get("))
        for verbo in ("post", "put", "delete", "patch", "request"):
            self.assertNotRegex(self.puerta, rf"_req\.{verbo}\(", f"_checkwms_get no puede usar {verbo}")

    def test_nadie_mas_llama_a_check_directo(self):
        # git ls-files: recorrer la carpeta entera en OneDrive tarda minutos
        salida = subprocess.run(["git", "ls-files", "*.py"], cwd=RAIZ,
                                capture_output=True, text=True, timeout=30).stdout
        for rel in salida.splitlines():
            ruta = os.path.join(RAIZ, rel)
            if rel.startswith("tests/") or not os.path.isfile(ruta):
                continue
            src = _leer(ruta)
            if "checkapi-integracion" not in src and "CHECKWMS_CONFIG[" not in src:
                continue
            if rel == "config.py":
                continue  # solo declara la URL base y lee las variables de entorno
            self.assertEqual(rel, "app.py", f"{rel} habla con Check directo")
            fuera = src.replace(self.puerta, "")
            self.assertNotRegex(fuera, r"CHECKWMS_CONFIG\[", "credenciales de Check usadas fuera de _checkwms_get")


if __name__ == "__main__":
    unittest.main()

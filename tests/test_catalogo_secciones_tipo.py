"""Ficha del Catálogo: qué secciones ve cada tipo de producto (Daniel, 25-oct-2026).

«Si el producto dice que es un accesorio, las piolas solamente las coloques para las selectoras de peso. Y las trotadoras
lo vamos a colocar en las medidas de cinta… Los manuales, si es accesorio, sácalo».

Qué se vigila:
  · catfTipoEquipo (static/catalogo_list.js, ejecutado con Node): accesorio / selectora / trotadora / otro.
  · catfAjustarSecciones: un dato ya cargado (piolas o manual) nunca se esconde (REGLA #4.2).
  · Las 4 columnas de la cinta: una cláusula por sentencia, revisadas antes (REGLA #18) y validadas en el PATCH.
Correr con:  python -m pytest -q tests/test_catalogo_secciones_tipo.py
"""
import json
import os
import re
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


JS = _leer("static/catalogo_list.js")


def _funcion(nombre):
    m = re.search(r"\nfunction " + nombre + r"\(.*?\n}\n", JS, re.S)
    assert m, nombre
    return m.group(0)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestTipoEquipo(unittest.TestCase):
    def _tipos(self, productos):
        code = _funcion("catfTipoEquipo") + "\nconsole.log(JSON.stringify(" + json.dumps(productos) + ".map(catfTipoEquipo)));"
        out = subprocess.run(["node", "-e", code], capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        return json.loads(out.stdout)

    def test_casos_reales(self):
        productos = [
            {"nombre": "Colchoneta Anti-Slip Pro ILUS", "familia": "Accesorios", "clase_producto_label": ""},
            {"nombre": "Prensa de pierna", "familia": "Fuerza", "clase_producto_label": "Selectoras de peso"},
            {"nombre": "Dual Cable Cross", "familia": "Fuerza", "clase_producto_label": "Dual Cable"},
            {"nombre": "Trotadora ILUS T900", "familia": "Cardio", "clase_producto_label": "Trotadoras"},
            {"nombre": "Treadmill Pro", "familia": "Cardio", "clase_producto_label": ""},
            {"nombre": "Banco ajustable", "familia": "Fuerza", "clase_producto_label": "Banco ajustable y fijo"},
            # «Accesorios» manda aunque el nombre hable de una cinta (p. ej. una cinta elástica).
            {"nombre": "Cinta elástica de resistencia", "familia": "Accesorios", "clase_producto_label": ""},
        ]
        self.assertEqual(self._tipos(productos),
                         ["accesorio", "selectora", "selectora", "trotadora", "trotadora", "otro", "accesorio"])


class TestReglas(unittest.TestCase):
    def test_un_dato_cargado_nunca_se_esconde(self):
        f = _funcion("catfAjustarSecciones")
        self.assertIn("tipo === 'selectora' || hayPiolas", f)
        self.assertIn("tipo !== 'accesorio' || hayManual", f)
        self.assertIn("verCinta = tipo === 'trotadora'", f)

    def test_las_secciones_tienen_id_y_la_cinta_existe(self):
        html = _leer("templates/catalogo/list.html")
        for i in ("catfEtapaPiolas", "catfEtapaCinta", "catfEtapaManuales", "catcLargo", "catcAncho", "catcGuardar"):
            self.assertIn('id="' + i + '"', html)

    def test_sin_dialogos_nativos(self):
        ini = JS.index("Secciones según el tipo de producto")
        bloque = JS[ini:JS.index("if (b) b.addEventListener('click', catcGuardar);", ini)]
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertIsNone(re.search(r"(?<![\w.])" + re.escape(nativo), bloque), nativo)

    def test_columnas_baratas_y_validadas(self):
        py = _leer("catalogo_module.py")
        for col in ("cinta_largo_mm", "cinta_ancho_mm", "cinta_espesor_mm", "cinta_obs"):
            m = re.search(r'\("' + col + r'",\s*"ALTER TABLE cat_productos ADD COLUMN ' + col + r' [^"]+"', py)
            self.assertIsNotNone(m, col)
            self.assertNotIn(",", m.group(0).split("ADD COLUMN", 1)[1].split("AFTER")[0].replace("(7,1)", "").replace("(6,1)", "").replace("(4,1)", ""))
        self.assertIn('"cinta_largo_mm": (500, 10000', py)
        self.assertIn("Revisa {_cinta_rangos[key][2]} de la cinta", py)



class TestActividad(unittest.TestCase):
    """«El historial puede quedar como registro de actividad… pero en el producto» (Daniel, 25-oct-2026)."""

    @classmethod
    def setUpClass(cls):
        cls.py = _leer("catalogo_module.py")

    def test_ruta_de_actividad_solo_lectura(self):
        i = self.py.index("def cat_api_actividad(")
        cuerpo = self.py[i:self.py.index("@app.route(", i)]
        self.assertIn('@app.route("/catalogo/api/productos/<int:pid>/actividad", methods=["GET"])\n    @_catalogo_required',
                      self.py)
        for escritura in ("mysql_execute", "INSERT", "UPDATE ", "DELETE"):
            self.assertNotIn(escritura, cuerpo)

    def test_cada_cambio_del_producto_queda_registrado(self):
        for accion in ("cat_producto_editar", "cat_foto_subir", "cat_foto_eliminar", "cat_manual_subir", "cat_manual_eliminar"):
            self.assertIn('_audit("' + accion + '"', self.py.replace("_audit('", '_audit("'))

    def test_tope_de_seis_piolas(self):
        self.assertIn("MAX_PIOLAS_POR_PRODUCTO = 6", self.py)
        self.assertIn("const MAX_PIOLAS = 6;", JS)

    def test_la_linea_de_tiempo_existe_y_escapa(self):
        html = _leer("templates/catalogo/list.html")
        for i in ("cataSection", "cataFiltros", "cataLista", "cataMas"):
            self.assertIn('id="' + i + '"', html)
        bloque = JS[JS.index("Registro de actividad del producto (2026-10-25)"):]
        self.assertIn("esc(ev.usuario", bloque)
        self.assertIn("esc(tx.d)", bloque)
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertIsNone(re.search(r"(?<![\w.])" + re.escape(nativo), bloque), nativo)


if __name__ == "__main__":
    unittest.main()

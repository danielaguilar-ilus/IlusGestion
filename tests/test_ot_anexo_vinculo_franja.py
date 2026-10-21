# -*- coding: utf-8 -*-
"""REGLA #34 — la franja «Un solo valor / No coincide» del vínculo costo ↔ anexo SE DIBUJA donde debe, también en una OT CERRADA.

Incidente real (2026-10-20, justo después de publicar el botón «Corregir con el anexo»): la franja vivía solo en el Paso 2 de la tarjeta
de edición, que únicamente se dibuja si `puede_metadata` (OT abierta). En las 4 OT cerradas que difieren de su anexo el botón nunca
aparecía: las pruebas de texto pasaban porque la frase estaba en la plantilla, pero la plantilla jamás la dibujaba para una OT cerrada.
Por eso aquí SE RENDERIZA el macro de verdad (jinja2) en cada situación, y se revisa que la ficha lo llame desde un lugar que existe
siempre (el bloque único «Costos y documentación»), no desde dentro de `{% if puede_metadata %}`.

Se corre con:  python -m pytest tests/test_ot_anexo_vinculo_franja.py -q
"""
import os
import re
import unittest

import jinja2

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RAIZ, "templates")), extensions=["jinja2.ext.do"])


def _franja(av, dom_id="otdAnexoVinculoFicha", vid=347, es_superadmin=False):
    mod = ENV.get_template("ot2/_anexo_vinculo.html").module
    return mod.franja(av, dom_id, vid, es_superadmin)


def _av(vinculo="difiere", firmado=True, abierta=False, numero=162, total_txt="$90.000", costo_txt="$80.000"):
    return {"id": 1, "numero": numero, "estado": "firmado" if firmado else "enviado", "firmado": firmado, "total": 90000,
            "total_txt": total_txt, "vinculo": vinculo, "costo_txt": costo_txt, "abierta": abierta}


def _txt(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


class TestFranjaSeDibuja(unittest.TestCase):
    def test_sin_anexo_vivo_no_dibuja_nada(self):
        self.assertEqual(_franja(None).strip(), "")

    def test_alineada_en_una_ot_abierta_explica_como_cambiarlo(self):
        h = _franja(_av("alineado", firmado=True, abierta=True, costo_txt="$90.000"), "otdAnexoVinculo", 239, False)
        t = _txt(h)
        self.assertIn("Un solo valor.", t)
        self.assertIn("Anexo N° 162 (firmado)", t)
        self.assertIn("emite un anexo nuevo", t)
        self.assertNotIn("<button", h)

    def test_alineada_en_una_ot_cerrada_no_da_consejos_de_edicion(self):
        t = _txt(_franja(_av("alineado", firmado=True, abierta=False), es_superadmin=True))
        self.assertIn("Un solo valor.", t)
        self.assertNotIn("Si cambia lo que se le paga", t)

    def test_distinta_en_una_ot_abierta_ofrece_usar_el_valor_del_anexo(self):
        h = _franja(_av("difiere", firmado=False, abierta=True), "otdAnexoVinculo", 239, False)
        self.assertIn("No coincide.", _txt(h))
        self.assertIn("otdCostoDesdeAnexo(239)", h)
        self.assertIn("Usar el valor del anexo", h)
        self.assertNotIn("otdCostoCorregirConAnexo", h)

    def test_sin_declarar_en_una_ot_abierta_tambien_ofrece_el_boton(self):
        h = _franja(_av("sin_declarar", abierta=True, costo_txt=""), "otdAnexoVinculo", 239, False)
        self.assertIn("aún no está", _txt(h))
        self.assertIn("otdCostoDesdeAnexo(239)", h)

    def test_ot_cerrada_con_anexo_firmado_el_superadmin_ve_corregir_con_el_anexo(self):
        h = _franja(_av("difiere", firmado=True, abierta=False), es_superadmin=True)
        self.assertIn("otdCostoCorregirConAnexo(347, 162)", h)
        self.assertIn("Corregir con el anexo", _txt(h))
        self.assertNotIn("otdCostoDesdeAnexo", h)                      # una OT cerrada es evidencia: no se "usa el valor" sin dejar registro

    def test_ot_cerrada_otro_rol_no_ve_ningun_boton(self):
        h = _franja(_av("difiere", firmado=True, abierta=False), es_superadmin=False)
        self.assertNotIn("<button", h)
        self.assertIn("No coincide.", _txt(h))                          # pero SÍ ve el aviso

    def test_ot_cerrada_con_anexo_sin_firmar_no_corrige_y_dice_por_que(self):
        h = _franja(_av("difiere", firmado=False, abierta=False, numero=160), es_superadmin=True)
        self.assertNotIn("<button", h)
        self.assertIn("todavía no está firmado", _txt(h))

    def test_el_id_es_el_que_pide_quien_llama(self):
        self.assertIn('id="otdAnexoVinculoFicha"', _franja(_av()))
        self.assertIn('id="otdAnexoVinculo"', _franja(_av(), "otdAnexoVinculo"))

    def test_no_usa_popups_nativos(self):
        h = _franja(_av("difiere", abierta=False), es_superadmin=True) + _franja(_av("difiere", abierta=True))
        for prohibido in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(prohibido, h)

    def test_botones_de_44_px_para_el_celular(self):
        h = _franja(_av("difiere", abierta=False), es_superadmin=True)
        self.assertIn("min-height:44px", h)


class TestLaFichaLoLlamaDondeExisteSiempre(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            cls.t = fh.read()

    def test_importa_el_macro(self):
        self.assertIn("{% from 'ot2/_anexo_vinculo.html' import franja as anexo_franja %}", self.t)

    def _ifs_abiertos(self, hasta):
        """Los `{% if … %}` que siguen abiertos justo antes de la posición `hasta` (desde `{% block content %}`), sin contar lo que está en
        comentarios Jinja. Sirve para saber DENTRO DE QUÉ condiciones queda una llamada: es lo que decide si se dibuja en una OT cerrada."""
        ini = self.t.index("{% block content %}")
        trozo = re.sub(r"\{#.*?#\}", "", self.t[ini:hasta], flags=re.S)
        pila = []
        for m in re.finditer(r"\{%-?\s*(if|endif)\b([^%]*?)-?%\}", trozo):
            if m.group(1) == "if":
                pila.append(m.group(2).strip())
            elif pila:
                pila.pop()
        return pila

    def test_el_bloque_unico_lo_dibuja_para_toda_ot_con_anexo(self):
        i = self.t.index("anexo_franja(anexo_vinculo, 'otdAnexoVinculoFicha'")
        # va dentro de la tarjeta del bloque único (#otdCardMotor), justo después del motor de finanzas...
        j_motor = self.t.rindex("{% include 'ot2/_fin_motor.html' %}", 0, i)
        j_tarjeta = self.t.rindex('id="otdCardMotor"', 0, j_motor)
        self.assertLess(j_tarjeta, j_motor)
        self.assertLess(i - j_motor, 900)
        # ...y NO dentro de ningún {% if puede_metadata %}: esa tarjeta de edición no existe en una OT cerrada (y ahí es donde se corrige).
        pila = self._ifs_abiertos(i)
        self.assertFalse([c for c in pila if "puede_metadata" in c], pila)
        # sí dentro de «no es técnico» (REGLAS #19 y #26) y de «hay un anexo vivo»
        self.assertTrue([c for c in pila if c == "not es_tecnico"], pila)
        self.assertTrue([c for c in pila if c == "anexo_vinculo"], pila)

    def test_el_paso_2_de_la_tarjeta_de_edicion_conserva_su_franja(self):
        # Quien edita (OT abierta) la tiene en las dos partes: el bloque único (visible) y el Paso 2 (dentro del modal «Editar cobro y costos»).
        paso2 = self.t.index("anexo_franja(anexo_vinculo, 'otdAnexoVinculo'")
        self.assertIn('data-paso="costo"', self.t[paso2 - 4000:paso2])
        self.assertTrue([c for c in self._ifs_abiertos(paso2) if "puede_metadata" in c])

    def test_los_ids_no_se_repiten(self):
        self.assertEqual(self.t.count("anexo_franja(anexo_vinculo, 'otdAnexoVinculo'"), 1)
        self.assertEqual(self.t.count("anexo_franja(anexo_vinculo, 'otdAnexoVinculoFicha'"), 1)
        self.assertNotIn('id="otdAnexoVinculo"', self.t)                     # el HTML vive solo en el macro

    def test_las_funciones_de_los_botones_existen_en_el_script_global(self):
        self.assertIn("window.otdCostoDesdeAnexo = async function", self.t)
        self.assertIn("window.otdCostoCorregirConAnexo = async function", self.t)

    def test_el_servidor_no_le_manda_el_vinculo_a_un_tecnico(self):
        # Además de que el bloque único no existe para técnicos, el dato mismo no viaja (REGLA #26: el dato se quita en el servidor).
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            s = fh.read()
        self.assertIn("anexo_vinculo = None if _es_rol_tecnico() else _ot_anexo_vinculo_vista(v)", s)

    def test_el_ayudante_de_pila_ve_bien_un_caso_conocido(self):
        # Salvaguarda de la propia prueba: la pila debe distinguir cerrar de abrir (si no, las pruebas de arriba no demostrarían nada).
        i = self.t.index("anexo_franja(anexo_vinculo, 'otdAnexoVinculoFicha'")
        self.assertNotEqual(self._ifs_abiertos(i), self._ifs_abiertos(self.t.index("anexo_franja(anexo_vinculo, 'otdAnexoVinculo'")))


if __name__ == "__main__":
    unittest.main()

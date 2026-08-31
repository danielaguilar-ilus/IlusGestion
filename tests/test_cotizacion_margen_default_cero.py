"""La cotización de Logística venía con "Margen % (global)" defaulteando a
30 en el formulario -- pese a que el backend (logistica_cotizaciones.py) ya
tenía margen_pct sembrado en "0" como política vigente ("🔴 Con margen_pct
en 0 (política vigente)", comentario ya existente cerca de la línea 2617).

Pedido de Daniel (2026-08-31, viendo en vivo el paso 6 "Condiciones de la
cotización" con el campo Margen % marcado en rojo mostrando "30"):
"Mira quiero que la cotizacion venga definido con 0% de margen por favor" y
luego, tras aclarar el alcance: "viene por definicion a 30% y definimos que
no debe venir con margen ya que fue anulado por la gerencia general" /
"es urgente que elimines eso por favor deben salir a 0 todas las sstt y las
de logistica".

El lado SSTT (Servicio Técnico, tk_settings.cotiz_margen_pct, editable en
vivo desde /catalogo/clases) ya estaba en 0 -- confirmado navegando a esa
pantalla con Chrome real logueado como Daniel. El lado Logística tenía DOS
huecos:

  1. El HTML/JS de _modal_cotizacion_logistica.html defaulteaba el input a
     30 (tanto el value estático como el reset al abrir una cotización
     nueva) -- pisaba el 0 correcto del backend en cada envío.
  2. logistica_cotizaciones.py, al agregar un ítem a una cotización YA
     creada, usaba `it["margen_pct_aplicado"] or cot.get("margen_pct") or
     30` -- con margen_pct en 0 (0 es falsy en Python), ese fallback caía
     igual en 30, silenciosamente, para cualquier ítem nuevo.

Correr con:  py -m unittest tests.test_cotizacion_margen_default_cero -v
"""
import unittest

with open("templates/transporte/_modal_cotizacion_logistica.html",
          encoding="utf-8", errors="ignore") as fh:
    HTML = fh.read()

with open("logistica_cotizaciones.py", encoding="utf-8", errors="ignore") as fh:
    PY = fh.read()


class TestElInputDeMargenYaNoDefaulteaA30(unittest.TestCase):

    def test_el_value_estatico_del_input_es_cero(self):
        i = HTML.index('id="lcMargenPct"')
        # El atributo value vive en la misma etiqueta <input>, antes del
        # siguiente ">".
        fin_tag = HTML.index(">", i)
        tag = HTML[max(0, i - 20):fin_tag]
        self.assertIn('value="0"', tag)
        self.assertNotIn('value="30"', tag)

    def test_el_reset_al_abrir_una_cotizacion_nueva_es_cero(self):
        self.assertIn("$('lcMargenPct').value = '0';", HTML)
        self.assertNotIn("$('lcMargenPct').value = '30';", HTML)

    def test_la_carga_de_una_cotizacion_existente_no_se_toco(self):
        # Fuera de alcance de este pedido: una cotizacion YA guardada con un
        # margen_pct viejo (de antes de la politica de 0%) debe seguir
        # mostrando su propio valor real -- el fallback a 30 ahi es solo
        # para registros historicos sin el campo poblado, no una regla
        # nueva. No se toca (REGLA #4.2).
        self.assertIn(
            "c.margen_pct != null ? c.margen_pct : 30", HTML)


class TestElFallbackDeAgregarItemsYaNoPisaElCeroReal(unittest.TestCase):

    def test_ya_no_queda_el_or_30_encadenado(self):
        self.assertNotIn('cot.get("margen_pct") or 30', PY)

    def test_usa_chequeos_explicitos_contra_none_no_falsy(self):
        i = PY.index("_margen_item = it[\"margen_pct_aplicado\"]")
        fragmento = PY[i:i + 400]
        self.assertIn('if _margen_item in (None, ""):', fragmento)
        self.assertIn('_margen_item = cot.get("margen_pct")', fragmento)
        self.assertIn("_margen_item = 0", fragmento)

    def test_el_insert_usa_la_variable_calculada_no_el_literal_30(self):
        # Hay varios INSERT INTO transport_cotizacion_items en el archivo
        # (crear cotizacion, clonar, agregar items) -- se ancla al de ESTE
        # fix buscando a partir de donde se calcula _margen_item.
        i_anchor = PY.index("_margen_item = it[\"margen_pct_aplicado\"]")
        i = PY.index("INSERT INTO transport_cotizacion_items", i_anchor)
        fragmento = PY[i:i + 700]
        self.assertIn("_margen_item,", fragmento)
        self.assertNotIn(" or 30", fragmento)

    def test_un_margen_global_en_cero_real_se_respeta_para_el_item_nuevo(self):
        """Simula la logica nueva linea por linea (sin levantar Flask/MySQL):
        cot.get('margen_pct') == 0 (politica vigente) e
        it['margen_pct_aplicado'] es None -> el item nuevo debe heredar 0,
        NUNCA 30."""
        cot = {"margen_pct": 0}
        it = {"margen_pct_aplicado": None}
        _margen_item = it["margen_pct_aplicado"]
        if _margen_item in (None, ""):
            _margen_item = cot.get("margen_pct")
        if _margen_item in (None, ""):
            _margen_item = 0
        self.assertEqual(_margen_item, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

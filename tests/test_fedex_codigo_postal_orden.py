"""BUG REAL EN PRODUCCIÓN (2026-08-13): FedEx rechazaba la FCV 11275
(destino Paine, RM) con "El código postal no es válido para el país
seleccionado" pese a que Paine SÍ está en la tabla oficial de códigos
postales (cl_codigos_postales.CODIGOS_POSTALES_CL, generada desde la
planilla oficial FedEx 2025, 481 comunas): 'paine': '9540000'.

De paso, Daniel mandó captura del MISMO modal (#trackingModal, endpoint
tr_item_tracking_detalle) mostrando "FCV 0000011275" con los ceros del
ERP a la izquierda. _doc_label ya existía para limpiar justo esto (pedido
de Daniel 2026-08-05) y se usa en ~10 lugares del módulo, pero este
endpoint se armó con el f-string crudo antes de que existiera el helper y
quedó afuera del barrido. Segundo fix incluido en este mismo archivo.

Causa raíz: dentro de _fedex_create_shipment (app.py), la resolución del
código postal probaba PRIMERO el dict viejo _COMUNA_POSTAL (56 comunas,
tipeado a mano) y solo si ese fallaba probaba la tabla oficial. El dict
viejo SÍ tenía a Paine, pero con el valor incorrecto 9680000 -- y como
"tenía algo", la tabla oficial (con el valor correcto) nunca llegaba a
intervenir.

Al comparar los 56 valores del dict viejo contra la tabla oficial,
34 de 56 (60%) están mal -- incluyendo Santiago, Providencia... no,
Santiago mismo (8320000 vs el oficial 2770000). El bug no era exclusivo
de Paine: afectaba potencialmente cualquier envío FedEx a esas 34 comunas.

Fix: la tabla oficial (_codigo_postal_chile / CODIGOS_POSTALES_CL) ahora
se prueba ANTES que el dict viejo (_comuna_to_postal), que queda como
último recurso solo para comunas que la tabla oficial no conozca.

Correr con:  py -m unittest tests.test_fedex_codigo_postal_orden -v
"""
import ast
import importlib.util
import os
import re
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_SRC = open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn_source(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontró la función {nombre} en app.py")


def _cargar_tabla_oficial():
    spec = importlib.util.spec_from_file_location(
        "cl_codigos_postales", os.path.join(BASE_DIR, "cl_codigos_postales.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CODIGOS_POSTALES_CL


def _extraer_comuna_postal_viejo():
    m = re.search(r"_COMUNA_POSTAL = \{(.*?)\n\}", APP_SRC, re.S)
    assert m, "no se encontró el dict _COMUNA_POSTAL en app.py"
    pares = re.findall(r'"([A-Z\xd1\xc1\xc9\xcd\xd3\xda ]+)":\s*"(\d+)"', m.group(1))
    return {k.strip(): v for k, v in pares}


def _norm(s):
    import unicodedata
    s = s.strip().lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


class TestOrdenResolucionCodigoPostal(unittest.TestCase):
    """La tabla oficial FedEx debe consultarse antes que el dict viejo."""

    def test_codigo_postal_chile_antes_que_comuna_to_postal(self):
        src = _fn_source("_fedex_create_shipment")
        pos_oficial = src.index("_codigo_postal_chile(rec_comuna)")
        pos_viejo = src.index("_comuna_to_postal(rec_comuna)")
        self.assertLess(
            pos_oficial, pos_viejo,
            "_codigo_postal_chile (tabla oficial, 481 comunas) debe probarse "
            "ANTES que _comuna_to_postal (dict viejo, 56 comunas, con errores "
            "conocidos) -- si no, el dict viejo vuelve a ganarle a la tabla "
            "oficial cada vez que ambos conocen la comuna.",
        )

    def test_cod_postal_del_recipient_sigue_siendo_lo_primero(self):
        # El código que viene directo del ERP (si vino) sigue siendo el más
        # confiable y no debe perder prioridad frente a ninguna tabla local.
        src = _fn_source("_fedex_create_shipment")
        pos_recipient = src.index('recipient.get(\'cod_postal\')')
        pos_oficial = src.index("_codigo_postal_chile(rec_comuna)")
        self.assertLess(pos_recipient, pos_oficial)


class TestDatosPaine(unittest.TestCase):
    """Documenta el caso concreto que reportó Daniel (FCV 11275)."""

    def test_paine_en_tabla_oficial_es_9540000(self):
        oficial = _cargar_tabla_oficial()
        self.assertEqual(oficial.get("paine"), "9540000")

    def test_paine_en_dict_viejo_estaba_mal(self):
        viejo = _extraer_comuna_postal_viejo()
        # Si algún día se corrige/borra la entrada vieja de Paine, este test
        # deja de tener sentido -- lo importante es que YA NO gane sobre la
        # tabla oficial (cubierto por el test de orden de arriba).
        if "PAINE" in viejo:
            self.assertNotEqual(
                viejo["PAINE"], "9540000",
                "si el dict viejo ahora coincide con la tabla oficial, "
                "actualiza este test -- ya no hace falta documentar el bug",
            )


class TestIntegridadTablaOficial(unittest.TestCase):
    """La tabla oficial (planilla FedEx 2025) es la fuente de verdad: debe
    cubrir siempre al menos las comunas que el dict viejo conocía."""

    def test_tabla_oficial_cubre_todas_las_comunas_del_dict_viejo(self):
        oficial = _cargar_tabla_oficial()
        viejo = _extraer_comuna_postal_viejo()
        # "PENARFLOR" es un typo pre-existente del dict viejo (falta la Ñ,
        # sobra una R) -- nunca hizo match ni antes de este fix, porque
        # _comuna_to_postal normaliza tildes pero no corrige typos. La tabla
        # oficial sí conoce la comuna bien escrita ("Peñaflor" -> "penaflor").
        # No es un caso nuevo que introduzca este fix, así que se excluye acá
        # en vez de tratarlo como regresión.
        conocidas_como_gap_preexistente = {"PENARFLOR"}
        faltantes = [
            k for k in viejo
            if _norm(k) not in oficial and k not in conocidas_como_gap_preexistente
        ]
        self.assertEqual(
            faltantes, [],
            f"comunas del dict viejo que la tabla oficial no conoce (revisar "
            f"si el fallback final _comuna_to_postal sigue siendo necesario): "
            f"{faltantes}",
        )


class TestDocLabelEnTrackingDetalle(unittest.TestCase):
    """El modal de tracking (#trackingModal) debe mostrar 'FCV 11275', no
    'FCV 0000011275' -- mismo estándar que ya usan los demás call sites."""

    def test_tracking_detalle_usa_doc_label(self):
        src = _fn_source("tr_item_tracking_detalle")
        self.assertIn(
            "_doc_label(row.get('tido'), row.get('nudo'))", src,
            "tr_item_tracking_detalle debe armar 'doc' con _doc_label, no con "
            "un f-string crudo -- si no, el nudo sale con los ceros del ERP.",
        )
        self.assertNotIn(
            "f\"{row.get('tido') or ''} {row.get('nudo') or ''}\".strip()", src,
            "quedó el f-string crudo viejo -- _doc_label debe reemplazarlo, no "
            "coexistir con él.",
        )

    def test_doc_label_quita_ceros_a_la_izquierda(self):
        # _doc_label es una función pura (sin DB/Flask): se puede extraer y
        # ejecutar aislada del resto de app.py.
        src = _fn_source("_doc_label")
        ns = {}
        exec(src, ns)
        doc_label = ns["_doc_label"]
        self.assertEqual(doc_label("FCV", "0000011275"), "FCV 11275")
        self.assertEqual(doc_label("BLV", "22890"), "BLV 22890")
        # Caso límite documentado en el propio docstring: no dejar vacío.
        self.assertEqual(doc_label("FCV", "0000000000"), "FCV 0000000000")


if __name__ == "__main__":
    unittest.main()

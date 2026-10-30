"""30-oct-2026 (Aarón: «la OT va lenta»): la trazabilidad de Random leía las guías de despacho con UNA consulta a MAEDDO por documento
(hasta 12 una tras otra, ~7 s la primera vez que una instancia abre la OT). Ahora es UNA consulta para todos los documentos y las guías
quedan 5 min en caché. Random sigue en SOLO LECTURA (REGLA #4.1): todo pasa por `_random_sql_query`. Base de mentira, sin ERP real.
Correr con: python -m unittest tests.test_ot_30oct_cadena_guias_lote"""
import time
import unittest

from tests import _fuente_app as FA

NOMBRES = ["_ot_cadena_guias", "_ot_cadena_guias_armar", "_ot_cadena_guias_lote"]


def _amb(filas_por_llamada):
    """Entorno con un `_random_sql_query` falso que devuelve las filas de cada llamada, en orden, y anota el SQL."""
    llamadas = []
    cola = list(filas_por_llamada)

    def falso(sql, params=None, max_rows=500):
        llamadas.append((sql, tuple(params or ()), max_rows))
        return cola.pop(0) if cola else []
    amb = FA.cargar(NOMBRES, extra={
        "_random_sql_query": falso, "time": time, "_OT_CADENA_MAX_GUIAS": 12, "_OT_CADENA_GUIAS_CACHE": {},
        "_OT_CADENA_GUIAS_TTL": 300, "_OT_CADENA_GUIAS_TOPE": 1500})
    return amb, llamadas


def _fila(ptido, pnudo, guia, sku="SKU1", cant=1, fecha="2026-10-01"):
    return {"ptido": ptido, "pnudo": pnudo, "guia": guia, "sku": sku, "producto": "Prod " + sku, "cantidad": cant, "fecha": fecha}


class TestGuiasEnLote(unittest.TestCase):
    def test_varios_documentos_van_en_una_sola_consulta(self):
        amb, llamadas = _amb([[_fila("NVV", "VD00010667", "33193"), _fila("FCV", "0000011439", "33200", cant=2)]])
        r = amb["_ot_cadena_guias_lote"]([("NVV", "VD00010667"), ("FCV", "0000011439"), ("BLV", "0000000001")])
        self.assertEqual(len(llamadas), 1, "una consulta para los 3 documentos")
        sql, params, _mr = llamadas[0]
        self.assertEqual(sql.count("d.TIDOPA = %s AND d.NUDOPA = %s"), 3)
        self.assertEqual(params, ("GDV", "NVV", "VD00010667", "FCV", "0000011439", "BLV", "0000000001"))
        self.assertEqual([g["numero"] for g in r[("NVV", "VD00010667")]], ["33193"])
        self.assertEqual(r[("FCV", "0000011439")][0]["productos"][0]["cantidad"], 2)
        self.assertEqual(r[("BLV", "0000000001")], [], "sin guías = lista vacía, no None")

    def test_la_segunda_vez_sale_de_la_cache(self):
        amb, llamadas = _amb([[_fila("NVV", "VD1", "1")]])
        amb["_ot_cadena_guias_lote"]([("NVV", "VD1")])
        r = amb["_ot_cadena_guias_lote"]([("NVV", "VD1")])
        self.assertEqual(len(llamadas), 1)
        self.assertEqual(r[("NVV", "VD1")][0]["numero"], "1")

    def test_si_random_no_responde_es_none_y_no_se_cachea(self):
        amb, llamadas = _amb([None, [_fila("NVV", "VD1", "1")]])
        self.assertIsNone(amb["_ot_cadena_guias_lote"]([("NVV", "VD1")])[("NVV", "VD1")])
        r = amb["_ot_cadena_guias_lote"]([("NVV", "VD1")])
        self.assertEqual(len(llamadas), 2, "el fallo no se queda en la caché")
        self.assertEqual(r[("NVV", "VD1")][0]["numero"], "1")

    def test_si_la_consulta_se_topa_con_el_maximo_se_lee_de_a_uno(self):
        tope = [_fila("NVV", "VD1", str(i)) for i in range(1500)]
        amb, llamadas = _amb([tope, [{"guia": "7", "sku": "A", "producto": "A", "cantidad": 1, "fecha": "2026-10-01"}]])
        r = amb["_ot_cadena_guias_lote"]([("NVV", "VD1")])
        self.assertEqual(len(llamadas), 2, "tope alcanzado: se repite como antes, un documento por consulta")
        self.assertEqual(r[("NVV", "VD1")][0]["numero"], "7")

    def test_solo_lectura_y_el_flujo_usa_el_lote(self):
        src = FA.fuente_funcion("_ot_cadena_guias_lote")
        self.assertIn("_random_sql_query(", src)
        for prohibido in ("INSERT", "UPDATE", "DELETE", "pymssql"):
            self.assertNotIn(prohibido, src)
        cad = FA.fuente_funcion("_ot_doc_cadena")
        self.assertIn("_ot_cadena_guias_lote(", cad)
        self.assertNotIn("g = _ot_cadena_guias(", cad, "ya no hay una consulta por documento dentro del bucle")


if __name__ == "__main__":
    unittest.main()

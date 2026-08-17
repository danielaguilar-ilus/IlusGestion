"""Pruebas de shipit_client.py — módulo puro (sin red, sin Flask, sin BD).

Fase 1 del plan de integración Shipit (pedido del área comercial, 2026-08-04):
solo cotizar tarifa, todavía no crear envíos. Cubre los 3 puntos que más
riesgo tienen de romperse en silencio:
  1. El payload de /v/rates exige TODOS los campos de dimensión/peso y los
     2 commune_id — si falta uno, la API responde 400 y hoy dependemos de
     detectarlo ANTES de llamar (build_rate_payload -> errores).
  2. La homologación de comuna (ERP con tilde/mayúscula mixta -> Shipit en
     mayúsculas sin tilde) es la pieza más fácil de romper con un caso real
     tipo "Ñuñoa" o "Peñalolén".
  3. parse_rates_response/lower_price nunca deben lanzar ante una respuesta
     rara (Shipit puede devolver prices:[] o lower_price:{} en un 400).

A diferencia de tests/test_simpliroute_payload.py (que no tiene bloque
__main__ y no corre con ningún comando del repo — deuda conocida), este
archivo usa unittest.TestCase para poder correr con:
    py -m unittest tests.test_shipit_payload -v
"""
import unittest

import shipit_client as sc


class TestNormalizarComuna(unittest.TestCase):
    def test_quita_tildes_y_pone_mayusculas(self):
        self.assertEqual(sc.normalizar_comuna("Ñuñoa"), "NUNOA")
        self.assertEqual(sc.normalizar_comuna("Peñalolén"), "PENALOLEN")
        self.assertEqual(sc.normalizar_comuna("Lo Barnechea"), "LO BARNECHEA")

    def test_colapsa_espacios_de_mas(self):
        self.assertEqual(sc.normalizar_comuna("  Las   Condes "), "LAS CONDES")

    def test_vacio_o_none(self):
        self.assertEqual(sc.normalizar_comuna(None), "")
        self.assertEqual(sc.normalizar_comuna(""), "")


class TestFindCommuneId(unittest.TestCase):
    COMUNAS = [
        {"id": 308, "name": "LAS CONDES", "is_available": True},
        {"id": 410, "name": "ACHAO", "is_available": False},
        {"id": 512, "name": "NUNOA", "is_available": True},
        {"id": 513, "name": "NUNOA", "is_available": False},  # duplicado sin cobertura
    ]

    def test_match_directo(self):
        cid, disp = sc.find_commune_id("Las Condes", self.COMUNAS)
        self.assertEqual(cid, 308)
        self.assertTrue(disp)

    def test_match_con_tilde_en_origen_erp(self):
        cid, disp = sc.find_commune_id("Ñuñoa", self.COMUNAS)
        self.assertEqual(cid, 512)
        self.assertTrue(disp)

    def test_sin_match_devuelve_none(self):
        cid, disp = sc.find_commune_id("Comuna Inexistente", self.COMUNAS)
        self.assertIsNone(cid)
        self.assertFalse(disp)

    def test_prefiere_el_disponible_si_hay_duplicados(self):
        # NUNOA aparece 2 veces (512 disponible, 513 no) -- debe ganar 512.
        cid, disp = sc.find_commune_id("NUNOA", self.COMUNAS)
        self.assertEqual(cid, 512)
        self.assertTrue(disp)

    def test_sin_comuna_o_sin_lista(self):
        self.assertEqual(sc.find_commune_id("", self.COMUNAS), (None, False))
        self.assertEqual(sc.find_commune_id("Las Condes", []), (None, False))
        self.assertEqual(sc.find_commune_id("Las Condes", None), (None, False))


class TestVerificarRestricciones(unittest.TestCase):
    """Daniel, 2026-08-04, textual: 'que no exceda de un bulto y que no
    exceda de los quince kilos... maneja la restricción indicada, o quince
    kilos o no es multibulto, dependiendo de cada caso -- no vayas a mandar
    algo genérico, algo específico.' Cada test verifica que el mensaje
    identifique la regla exacta, no un texto genérico de rechazo.

    2026-08-17: el tope de peso pasó de 15 a 20 kg a pedido de Daniel
    ("aumentemos la restricción de Shipit de 15 a 20 kg para realizar
    pruebas"). Los tests se leen desde sc.MAX_PESO_KG en vez de tener el
    número escrito a mano: así el próximo ajuste del tope no obliga a
    reescribirlos, y lo que se verifica es la REGLA (mensaje específico por
    causa), que es lo que Daniel pidió y no cambió.
    """

    def test_dentro_de_limites_no_hay_problemas(self):
        self.assertEqual(sc.verificar_restricciones(1, sc.MAX_PESO_KG - 0.1), [])
        self.assertEqual(sc.verificar_restricciones(1, sc.MAX_PESO_KG), [])  # inclusive

    def test_el_tope_vigente_es_20_kg(self):
        # Fija la decisión del 2026-08-17. Si alguien lo vuelve a mover, que
        # sea a propósito y con este test enfrente.
        self.assertEqual(sc.MAX_PESO_KG, 20.0)

    def test_una_barra_de_20_kg_ahora_si_pasa(self):
        # Era el caso que Daniel aceptó dejar fuera en agosto y que ahora
        # quiere poder probar por Shipit.
        self.assertEqual(sc.verificar_restricciones(1, 20), [])

    def test_multibulto_reporta_mensaje_especifico_de_bultos(self):
        problemas = sc.verificar_restricciones(3, 5)
        self.assertEqual(len(problemas), 1)
        self.assertIn("3 bultos", problemas[0])
        self.assertIn("1 bulto", problemas[0])

    def test_sobrepeso_reporta_mensaje_especifico_de_kilos_no_de_bultos(self):
        sobre = sc.MAX_PESO_KG + 5      # 25 kg con el tope actual
        problemas = sc.verificar_restricciones(1, sobre)
        self.assertEqual(len(problemas), 1)
        self.assertIn(f"{sobre:g} kg", problemas[0])
        self.assertIn(f"{sc.MAX_PESO_KG:g}", problemas[0])
        self.assertNotIn("bulto", problemas[0].split("—")[0])  # el motivo no mezcla bultos

    def test_ambas_restricciones_a_la_vez_devuelve_dos_mensajes_separados(self):
        problemas = sc.verificar_restricciones(2, sc.MAX_PESO_KG + 5)
        self.assertEqual(len(problemas), 2)
        self.assertTrue(any("bultos" in p for p in problemas))
        self.assertTrue(any("kg" in p for p in problemas))

    def test_valores_ausentes_no_generan_falso_positivo(self):
        self.assertEqual(sc.verificar_restricciones(None, None), [])


class TestBuildRatePayload(unittest.TestCase):
    def test_payload_valido(self):
        payload, errores = sc.build_rate_payload(
            length=10, width=10, height=10, weight=1.5,
            origin_id=308, destiny_id=410,
        )
        self.assertEqual(errores, [])
        self.assertEqual(payload, {"parcel": {
            "length": 10.0, "width": 10.0, "height": 10.0, "weight": 1.5,
            "origin_id": 308, "destiny_id": 410, "type_of_destiny": "domicilio",
        }})

    def test_type_of_destiny_por_defecto_es_domicilio(self):
        payload, _ = sc.build_rate_payload(
            length=1, width=1, height=1, weight=1, origin_id=1, destiny_id=1)
        self.assertEqual(payload["parcel"]["type_of_destiny"], "domicilio")

    def test_courier_for_client_opcional(self):
        payload, _ = sc.build_rate_payload(
            length=1, width=1, height=1, weight=1, origin_id=1, destiny_id=1,
            courier_for_client="chilexpress")
        self.assertEqual(payload["parcel"]["courier_for_client"], "chilexpress")

    def test_sin_commune_id_de_origen_o_destino_reporta_error_accionable(self):
        # Este es EL caso real: comuna del ERP que aun no fue homologada
        # contra /v/communes -- debe fallar ANTES de llamar a la API, con
        # un mensaje que diga "falta homologar", no un 400 opaco de Shipit.
        _, errores = sc.build_rate_payload(
            length=1, width=1, height=1, weight=1, origin_id=None, destiny_id=None)
        self.assertTrue(any("origen" in e for e in errores))
        self.assertTrue(any("destino" in e for e in errores))

    def test_dimension_cero_o_negativa_es_invalida(self):
        _, errores = sc.build_rate_payload(
            length=0, width=-5, height=10, weight=1, origin_id=1, destiny_id=1)
        self.assertTrue(any("largo" in e for e in errores))
        self.assertTrue(any("ancho" in e for e in errores))

    def test_peso_cero_es_invalido(self):
        _, errores = sc.build_rate_payload(
            length=1, width=1, height=1, weight=0, origin_id=1, destiny_id=1)
        self.assertTrue(any("peso" in e for e in errores))


class TestParseRatesResponse(unittest.TestCase):
    RESPONSE_REAL = {
        "algorithm": "1", "algorithm_days": "2", "courier_for_client": None,
        "prices": [
            {
                "courier": {"name": "chilexpress", "packet_from": "LAS CONDES", "packet_to": "LAS CONDES"},
                "name": "DIA HABIL SIGUIENTE", "price": 3915, "days": 1,
                "available_to_shipping": True, "original_courier": "chilexpress",
                "volumetric_weight": 1.0,
                "destiny": {"id": 4472, "commune_id": 308, "type_of_destiny": "domicilio"},
            },
        ],
        "lower_price": {
            "courier": {"name": "starken", "packet_from": "LAS CONDES", "packet_to": "LAS CONDES"},
            "name": "DIA HABIL SIGUIENTE", "price": 3803, "days": 1,
            "available_to_shipping": True, "original_courier": "starken",
            "volumetric_weight": 1.0,
            "destiny": {"id": 14762, "commune_id": 308, "type_of_destiny": "domicilio"},
        },
    }

    def test_parsea_respuesta_real_documentada(self):
        rows = sc.parse_rates_response(self.RESPONSE_REAL)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], {
            "courier": "chilexpress", "servicio": "DIA HABIL SIGUIENTE",
            "precio": 3915.0, "dias": 1, "peso_volumetrico": 1.0,
            "disponible": True, "commune_destino_id": 308,
        })

    def test_lower_price_extrae_la_opcion_mas_barata(self):
        lp = sc.lower_price(self.RESPONSE_REAL)
        self.assertEqual(lp["courier"], "starken")
        self.assertEqual(lp["precio"], 3803.0)

    def test_respuesta_de_error_400_no_lanza_y_devuelve_vacio(self):
        # Shape real documentado del 400: prices:[] y lower_price:{}
        error_400 = {"courier_for_client": "", "prices": [], "lower_price": {},
                     "algorithm": "", "algorithm_days": "",
                     "message": "Destino no encontrado", "state": "error"}
        self.assertEqual(sc.parse_rates_response(error_400), [])
        self.assertIsNone(sc.lower_price(error_400))

    def test_data_rara_nunca_lanza_y_devuelve_vacio(self):
        for rara in (None, {}, [], "texto", 42, {"prices": "no es lista"}, {"prices": None}):
            self.assertEqual(sc.parse_rates_response(rara), [],
                              msg=f"input rechazado incorrectamente: {rara!r}")

    def test_basura_mezclada_dentro_de_prices_se_descarta_sin_lanzar(self):
        rows = sc.parse_rates_response({"prices": [None, 42, "texto", {"courier": {}, "price": 100}]})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["precio"], 100.0)


class TestExtractErrorMessage(unittest.TestCase):
    def test_401_403_404_tienen_mensaje_accionable(self):
        self.assertIn("SHIPIT_API_TOKEN", sc.extract_error_message(401, None))
        self.assertIn("permisos", sc.extract_error_message(403, None))
        self.assertIn("404", sc.extract_error_message(404, None))

    def test_400_con_shape_documentado(self):
        body = {"message": "Destino no encontrado", "state": "error"}
        self.assertEqual(sc.extract_error_message(400, body), "Destino no encontrado")

    def test_dict_generico_sin_message(self):
        body = {"weight": ["no puede estar en blanco"]}
        msg = sc.extract_error_message(400, body)
        self.assertIn("weight", msg)

    def test_nunca_lanza_con_cuerpo_vacio(self):
        self.assertEqual(sc.extract_error_message(500, None), "Error HTTP 500")
        self.assertEqual(sc.extract_error_message(500, ""), "Error HTTP 500")


if __name__ == "__main__":
    unittest.main(verbosity=2)

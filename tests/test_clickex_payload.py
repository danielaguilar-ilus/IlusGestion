"""Pruebas de clickex_client.py — módulo puro (sin red, sin Flask, sin BD).

Fase 1 del plan de integración Clickex (pedido de Daniel, 2026-08-24/25 vía
chat): "instalemos y activemos Clickex... que se pueda cotizar, se pueda
llamar a crear un pedido, se pueda consultar los estados y se pueda traer
los precios". Mismo patrón de test que tests/test_shipit_payload.py -- corre
con:
    py -m unittest tests.test_clickex_payload -v

Datos reales verificados en vivo contra la API (23/24-08-2026, cuenta
sport_api_integration@clickex.cl, GET /sellerShipmentMatrixCosts): 37 filas,
todas Región Metropolitana, tarifa plana $3.520, SLA 1 día. Incluye "Ñuñoa" y
"Nunoa" como dos filas SEPARADAS con el mismo precio -- caso real que
find_tarifa_comuna debe resolver sin fusionar ni romperse.
"""
import unittest

import clickex_client as clc


class TestNormalizarComuna(unittest.TestCase):
    def test_quita_tildes_y_pone_mayusculas(self):
        self.assertEqual(clc.normalizar_comuna("Ñuñoa"), "NUNOA")
        self.assertEqual(clc.normalizar_comuna("Peñalolén"), "PENALOLEN")
        self.assertEqual(clc.normalizar_comuna("Lo Barnechea"), "LO BARNECHEA")

    def test_colapsa_espacios_de_mas(self):
        self.assertEqual(clc.normalizar_comuna("  Las   Condes "), "LAS CONDES")

    def test_vacio_o_none(self):
        self.assertEqual(clc.normalizar_comuna(None), "")
        self.assertEqual(clc.normalizar_comuna(""), "")


class TestVerificarRestricciones(unittest.TestCase):
    """Daniel, 2026-08-24, textual: 'con respecto al límite, sí, te confirmo
    que son veinticinco kilos' -- y, a diferencia de Shipit, pidió NO
    limitar dimensiones ('no limitaría las barras... no tomaría en cuenta
    esas dimensiones')."""

    def test_bajo_el_limite_no_hay_problemas(self):
        self.assertEqual(clc.verificar_restricciones(24.9), [])
        self.assertEqual(clc.verificar_restricciones(25.0), [])

    def test_sobre_el_limite_da_mensaje_especifico(self):
        problemas = clc.verificar_restricciones(30)
        self.assertEqual(len(problemas), 1)
        self.assertIn("25", problemas[0])
        self.assertIn("kg", problemas[0])

    def test_no_hay_restriccion_de_dimensiones(self):
        # A diferencia de Shipit (MAX_BULTOS), Clickex no valida bultos ni
        # medidas -- solo la firma de la función acepta peso_kg.
        self.assertEqual(clc.verificar_restricciones(24), [])

    def test_peso_none_no_lanza(self):
        self.assertEqual(clc.verificar_restricciones(None), [])


class TestParseMatrixCostsResponse(unittest.TestCase):
    RESPUESTA_REAL = {
        "costs": [
            {"commune": "Las Condes", "sla": "1", "net_cost": "3520"},
            {"commune": "Ñuñoa", "sla": "1", "net_cost": "3520"},
            {"commune": "Nunoa", "sla": "1", "net_cost": "3520"},
        ]
    }

    def test_parsea_la_lista_completa(self):
        out = clc.parse_matrix_costs_response(self.RESPUESTA_REAL)
        self.assertEqual(len(out), 3)
        self.assertEqual(out[0]["comuna"], "Las Condes")
        self.assertEqual(out[0]["sla_dias"], 1)
        self.assertEqual(out[0]["costo_neto"], 3520.0)

    def test_incluye_comuna_normalizada(self):
        out = clc.parse_matrix_costs_response(self.RESPUESTA_REAL)
        self.assertEqual(out[1]["comuna_normalizada"], "NUNOA")
        self.assertEqual(out[2]["comuna_normalizada"], "NUNOA")

    def test_data_rara_nunca_lanza(self):
        self.assertEqual(clc.parse_matrix_costs_response(None), [])
        self.assertEqual(clc.parse_matrix_costs_response({}), [])
        self.assertEqual(clc.parse_matrix_costs_response({"costs": "no es lista"}), [])
        self.assertEqual(clc.parse_matrix_costs_response({"costs": [1, 2, "x"]}), [])

    def test_fila_sin_commune_se_descarta(self):
        out = clc.parse_matrix_costs_response({"costs": [{"sla": "1", "net_cost": "100"}]})
        self.assertEqual(out, [])


class TestFindTarifaComuna(unittest.TestCase):
    TARIFAS = clc.parse_matrix_costs_response(
        TestParseMatrixCostsResponse.RESPUESTA_REAL)

    def test_match_directo(self):
        t = clc.find_tarifa_comuna("Las Condes", self.TARIFAS)
        self.assertIsNotNone(t)
        self.assertEqual(t["costo_neto"], 3520.0)

    def test_match_con_tilde_del_erp_contra_fila_sin_tilde(self):
        # El ERP manda "Ñuñoa" con tilde; la matriz trae la fila "Nunoa" sin
        # tilde en la posición [2] -- normalizar_comuna debe igualarlas.
        t = clc.find_tarifa_comuna("Ñuñoa", self.TARIFAS)
        self.assertIsNotNone(t)
        self.assertEqual(t["costo_neto"], 3520.0)

    def test_sin_cobertura_devuelve_none(self):
        self.assertIsNone(clc.find_tarifa_comuna("Comuna Inexistente", self.TARIFAS))

    def test_sin_comuna_o_sin_lista(self):
        self.assertIsNone(clc.find_tarifa_comuna("", self.TARIFAS))
        self.assertIsNone(clc.find_tarifa_comuna("Las Condes", []))
        self.assertIsNone(clc.find_tarifa_comuna("Las Condes", None))


class TestAritmeticaReplicadaMatrizReal(unittest.TestCase):
    """Los 37 pares comuna/precio reales devueltos por la API en vivo
    (23-08-2026), guardados en la sesión anterior -- confirma que TODAS las
    comunas de la cuenta ILUS traen la misma tarifa plana."""

    COMUNAS_REALES = [
        "Cerrillos", "Cerro Navia", "Colina", "Conchalí", "El Bosque",
        "Estación Central", "Huechuraba", "Independencia", "La Cisterna",
        "La Florida", "La Granja", "La Pintana", "La Reina", "Las Condes",
        "Lo Barnechea", "Lo Espejo", "Lo Prado", "Macul", "Maipú", "Ñuñoa",
        "Nunoa", "Padre Hurtado", "Pedro Aguirre Cerda", "Peñalolén",
        "Providencia", "Pudahuel", "Puente Alto", "Quilicura",
        "Quinta Normal", "Recoleta", "Renca", "San Bernardo", "San Joaquín",
        "San Miguel", "San Ramón", "Santiago", "Vitacura",
    ]

    def test_37_comunas_todas_a_3520(self):
        self.assertEqual(len(self.COMUNAS_REALES), 37)
        data = {"costs": [{"commune": c, "sla": "1", "net_cost": "3520"}
                           for c in self.COMUNAS_REALES]}
        out = clc.parse_matrix_costs_response(data)
        self.assertEqual(len(out), 37)
        self.assertTrue(all(f["costo_neto"] == 3520.0 for f in out))
        self.assertTrue(all(f["sla_dias"] == 1 for f in out))

    def test_nunoa_y_nunoa_conviven_como_filas_separadas(self):
        # Caso real: la API devuelve "Ñuñoa" y "Nunoa" como dos filas -- no
        # se deben fusionar en el parseo (eso lo decide find_tarifa_comuna
        # al buscar, quedándose con la primera que matchea).
        data = {"costs": [{"commune": c, "sla": "1", "net_cost": "3520"}
                           for c in self.COMUNAS_REALES]}
        out = clc.parse_matrix_costs_response(data)
        normalizadas = [f["comuna_normalizada"] for f in out]
        self.assertEqual(normalizadas.count("NUNOA"), 2)


class TestBuildShipmentPayload(unittest.TestCase):
    """POST /shipmentsAdd -- módulo puro, SIN caller todavía (ver app.py:
    no hay ningún botón que dispare esto en Fase 1). Se prueba igual porque
    debe estar listo y correcto para cuando Daniel confirme activarlo."""

    def _base_kwargs(self, **overrides):
        kwargs = dict(
            nombre_dest="Cliente Ejemplo", email_dest="cliente@example.com",
            telefono_dest="+56912345678", comuna="Providencia",
            calle="Av. Providencia", numero="1234", complemento="",
            peso_total=10, total_bultos=1,
        )
        kwargs.update(overrides)
        return kwargs

    def test_payload_valido_sin_errores(self):
        payload, errores = clc.build_shipment_payload(**self._base_kwargs())
        self.assertEqual(errores, [])
        self.assertEqual(payload["addressee"]["name"], "Cliente Ejemplo")
        self.assertEqual(payload["address"]["commune"], "Providencia")
        self.assertEqual(payload["Package"]["totalWeight"], 10)
        self.assertEqual(payload["Package"]["totalPackages"], 1)

    def test_complement_siempre_presente_aunque_vacio(self):
        # El spec marca 'complement' como obligatorio en 'address', aunque
        # sea texto vacío -- si se omite la llave, Clickex puede rechazar
        # con 400 "Datos obligatorios faltantes".
        payload, _ = clc.build_shipment_payload(**self._base_kwargs())
        self.assertIn("complement", payload["address"])

    def test_falta_nombre_destinatario(self):
        _, errores = clc.build_shipment_payload(**self._base_kwargs(nombre_dest=""))
        self.assertTrue(any("nombre" in e for e in errores))

    def test_falta_email_destinatario(self):
        _, errores = clc.build_shipment_payload(**self._base_kwargs(email_dest=""))
        self.assertTrue(any("email" in e for e in errores))

    def test_falta_telefono_destinatario(self):
        _, errores = clc.build_shipment_payload(**self._base_kwargs(telefono_dest=""))
        self.assertTrue(any("teléfono" in e for e in errores))

    def test_falta_comuna_calle_o_numero(self):
        _, e1 = clc.build_shipment_payload(**self._base_kwargs(comuna=""))
        _, e2 = clc.build_shipment_payload(**self._base_kwargs(calle=""))
        _, e3 = clc.build_shipment_payload(**self._base_kwargs(numero=""))
        self.assertTrue(any("comuna" in e for e in e1))
        self.assertTrue(any("calle" in e for e in e2))
        self.assertTrue(any("número" in e for e in e3))

    def test_peso_invalido(self):
        _, errores = clc.build_shipment_payload(**self._base_kwargs(peso_total=0))
        self.assertTrue(any("peso" in e for e in errores))

    def test_peso_sobre_25kg_agrega_la_restriccion_de_negocio(self):
        _, errores = clc.build_shipment_payload(**self._base_kwargs(peso_total=30))
        self.assertTrue(any("25" in e and "kg" in e for e in errores))

    def test_campos_opcionales_solo_se_agregan_si_vienen(self):
        payload, _ = clc.build_shipment_payload(**self._base_kwargs())
        self.assertNotIn("sender", payload)
        self.assertNotIn("referenciaEnvio", payload)
        payload2, _ = clc.build_shipment_payload(
            **self._base_kwargs(nombre_remitente="ILUS Fitness", referencia="OT-123"))
        self.assertEqual(payload2["sender"]["name"], "ILUS Fitness")
        self.assertEqual(payload2["referenciaEnvio"], "OT-123")


class TestParseShipmentResponse(unittest.TestCase):
    def test_data_rara_nunca_lanza(self):
        self.assertEqual(clc.parse_shipment_response(None), {"tracking": None, "raw": None})
        self.assertEqual(clc.parse_shipment_response("texto")["tracking"], None)

    def test_extrae_tracking_si_viene(self):
        r = clc.parse_shipment_response({"tracking": "202504455821"})
        self.assertEqual(r["tracking"], "202504455821")


class TestParseTrackingResponse(unittest.TestCase):
    RESPUESTA_REAL_SHAPE = {
        "response": {
            "tracking": {"status": "en camino"},
            "shipments_status_log": [{"status": "creado"}, {"status": "en camino"}],
            "checkout_observation": "dejar en conserjería",
        }
    }

    def test_parsea_los_3_campos(self):
        r = clc.parse_tracking_response(self.RESPUESTA_REAL_SHAPE)
        self.assertEqual(r["tracking_info"], {"status": "en camino"})
        self.assertEqual(len(r["historial"]), 2)
        self.assertEqual(r["observacion"], "dejar en conserjería")

    def test_data_rara_nunca_lanza(self):
        self.assertEqual(clc.parse_tracking_response(None),
                          {"tracking_info": {}, "historial": [], "observacion": ""})
        self.assertEqual(clc.parse_tracking_response({}),
                          {"tracking_info": {}, "historial": [], "observacion": ""})
        self.assertEqual(clc.parse_tracking_response({"response": "no es dict"}),
                          {"tracking_info": {}, "historial": [], "observacion": ""})


class TestExtractErrorMessage(unittest.TestCase):
    def test_401_da_mensaje_especifico_de_api_key(self):
        msg = clc.extract_error_message(401, {"error": "unauthorized"})
        self.assertIn("CLICKEX_API_KEY", msg)

    def test_404_da_mensaje_especifico(self):
        msg = clc.extract_error_message(404, {"error": "not found"})
        self.assertIn("404", msg)

    def test_usa_el_campo_error_del_body(self):
        msg = clc.extract_error_message(400, {"error": "Faltan datos obligatorios"})
        self.assertEqual(msg, "Faltan datos obligatorios")

    def test_body_sin_forma_esperada_no_lanza(self):
        self.assertTrue(clc.extract_error_message(500, None))
        self.assertTrue(clc.extract_error_message(500, "texto plano"))
        self.assertTrue(clc.extract_error_message(500, {}))


if __name__ == "__main__":
    unittest.main()

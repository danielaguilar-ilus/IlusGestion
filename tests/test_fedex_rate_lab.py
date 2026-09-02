"""Laboratorio de cotización FedEx (/transporte/api/diagnostico/fedex-rate-lab).

Pedido de Daniel (2026-09-01), caso real FCV 11431 (Copiapó, 13 bultos,
1.127,451 kg): la tabla interna "FedEx Directo" cotiza $448.621, pero FedEx
cobró de verdad $736.020 con el servicio "FedEx Carga Prioritaria" (portal
ShippingPlus). Al probar la Rate API real (ver conversación), devolvía
FEDEX_PRIORITY_EXPRESS (paquetería, ~$6.650/kg) y HTTP 400 al mandar el peso
total como UN solo bulto.

Hay 4 sospechosos independientes (serviceType carga vs paquetería, multibulto
real vs bulto único, rateRequestType ACCOUNT vs LIST, código postal real vs
la aproximación por zona) y ninguno confirmado. Este endpoint (SOLO
superadmin, SOLO cotiza -- nunca crea envíos) permite mandar variantes del
request a la Rate API real y ver la respuesta CRUDA para encontrar
empíricamente la combinación correcta, antes de tocar el cotizador de
producción (_fedex_calc_rate / api_asignar_tarifa_fedex), que este endpoint
NO modifica.

app.py tiene 90k+ líneas -- se extrae el cuerpo de las funciones nuevas por
slicing de texto, mismo patrón que el resto de los tests de este módulo.

Correr con:  py -m unittest tests.test_fedex_rate_lab -v
"""
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
    SRC = f.read()


def _fuente_funcion(nombre):
    i = SRC.index(f"\ndef {nombre}(")
    j_def = SRC.index("\ndef ", i + 10)
    # "\ndef " deja ADENTRO del slice los decoradores de la SIGUIENTE ruta
    # (preceden al "def " siguiente en el texto crudo) -- si alguno de esos
    # decoradores menciona texto que un test busca en la funcion actual (ej.
    # el path de la ruta siguiente), da un falso positivo. Se recorta antes
    # si hay un "@app.route" entre medio.
    j_route = SRC.find("\n@app.route", i + 10, j_def)
    j = j_route if j_route != -1 else j_def
    return SRC[i:j]


def _fuente_entre(marca_inicio, marca_fin):
    i = SRC.index(marca_inicio)
    j = SRC.index(marca_fin, i)
    return SRC[i:j]


ENDPOINT = _fuente_funcion("tr_diag_fedex_rate_lab")
VARIANTES = _fuente_entre("_FEDEX_LAB_VARIANTES = {", "\ndef _fedex_lab_bultos_de_doc")
BULTOS_DOC = _fuente_funcion("_fedex_lab_bultos_de_doc")
NORMALIZAR = _fuente_funcion("_fedex_lab_normalizar_bultos")


class TestSoloSuperadminYSoloLectura(unittest.TestCase):
    """Garantías no negociables del laboratorio: nunca lo puede usar alguien
    sin superadmin, y nunca puede crear un envío real."""

    def test_gate_exige_superadmin(self):
        # El docstring de la funcion es largo (explica uso rapido/avanzado);
        # el chequeo real viene despues. Se ancla directo al if, no al inicio
        # de la funcion.
        i = ENDPOINT.index('if not g.permissions.get("superadmin")')
        fragmento = ENDPOINT[i:i + 200]
        self.assertIn("403", fragmento)

    def test_no_usa_tr_required_generico(self):
        # @_tr_required exige el permiso 'transporte' y redirige (no es un
        # 403 JSON) -- este endpoint debe validar superadmin EXPLICITO
        # dentro del cuerpo, no delegarlo a un decorador mas laxo.
        i = SRC.index("def tr_diag_fedex_rate_lab")
        decoradores = SRC[max(0, i - 200):i]
        self.assertIn("@login_required", decoradores)
        self.assertNotIn("@_tr_required", decoradores)

    def test_no_llama_a_ningun_endpoint_de_creacion_de_envio(self):
        # Ni Ship API, ni creacion de OT, ni escritura en transport_manifests.
        self.assertNotIn("_fedex_create_shipment", ENDPOINT)
        self.assertNotIn("crear-ot-fedex", ENDPOINT)
        self.assertNotIn("INSERT INTO", ENDPOINT)
        self.assertNotIn("UPDATE transport", ENDPOINT)
        self.assertNotIn("conn.commit()", ENDPOINT)

    def test_solo_hace_un_post_al_endpoint_de_rate_quotes(self):
        self.assertIn("FEDEX_RATE_URL", ENDPOINT)
        self.assertIn('_req.post(', ENDPOINT)
        # Ningun otro verbo HTTP contra apis.fedex.com desde este endpoint.
        self.assertNotIn("_req.put(", ENDPOINT)
        self.assertNotIn("_req.delete(", ENDPOINT)

    def test_la_respuesta_declara_explicitamente_que_es_solo_lectura(self):
        self.assertIn('"solo_lectura"', ENDPOINT)


class TestNoTocaElCotizadorDeProduccion(unittest.TestCase):
    """REGLA #4.2: el laboratorio convive con _fedex_calc_rate, no lo
    reemplaza ni lo modifica."""

    def test_fedex_calc_rate_sigue_existiendo_intacta(self):
        fuente = _fuente_funcion("_fedex_calc_rate")
        self.assertIn("requestedPackageLineItems", fuente)
        self.assertIn("groupPackageCount", fuente)

    def test_api_asignar_tarifa_fedex_sigue_llamando_a_fedex_calc_rate(self):
        fuente = _fuente_funcion("api_asignar_tarifa_fedex")
        self.assertIn("_fedex_calc_rate(peso_pred, postal_dest", fuente)

    def test_el_laboratorio_no_se_llama_desde_ningun_lado_de_produccion(self):
        # No debe quedar cableado al comparador ni a ninguna pantalla que
        # vean Alison/vendedores -- es una herramienta de diagnostico. Busca
        # en JS/templates (archivos EXTERNOS a app.py), no en app.py mismo --
        # el propio docstring del endpoint menciona su ruta como ejemplo de
        # uso, lo que da falso positivo si se busca dentro de app.py.
        import glob
        referenciado = []
        for patron in ("static/**/*.js", "templates/**/*.html"):
            for ruta in glob.glob(os.path.join(BASE_DIR, patron), recursive=True):
                with open(ruta, encoding="utf-8", errors="ignore") as fh:
                    if "fedex-rate-lab" in fh.read():
                        referenciado.append(ruta)
        self.assertEqual(referenciado, [],
            f"el laboratorio de diagnostico esta cableado desde: {referenciado}")


class TestReusaElOAuthExistente(unittest.TestCase):
    def test_reusa_fedex_get_token_no_duplica_oauth(self):
        self.assertIn("_fedex_get_token()", ENDPOINT)
        self.assertNotIn("FEDEX_OAUTH_URL", ENDPOINT)


class TestNuncaExponeCredenciales(unittest.TestCase):
    """REGLA #4: ningun secreto crudo debe poder llegar al navegador."""

    def test_toda_respuesta_cruda_pasa_por_sanitizar_log(self):
        i = ENDPOINT.index("def _sanear")
        fragmento = ENDPOINT[i:i + 400] if i > -1 else ""
        # _sanear() esta definida DENTRO del endpoint (closure) -- se busca
        # en el cuerpo completo.
        self.assertIn("_fedex_sanitizar_log", ENDPOINT)

    def test_respuesta_cruda_de_fedex_pasa_por_sanear(self):
        i = ENDPOINT.index('"respuesta_cruda"')
        fragmento = ENDPOINT[max(0, i - 60):i + 40]
        self.assertIn("_sanear(data)", fragmento)

    def test_la_cuenta_fedex_viaja_enmascarada(self):
        i = ENDPOINT.index('"cuenta_fedex_usada"')
        fragmento = ENDPOINT[i:i + 200]
        self.assertIn("FEDEX_ACCOUNT[:2]", fragmento)
        self.assertIn("FEDEX_ACCOUNT[-2:]", fragmento)
        self.assertNotIn("FEDEX_ACCOUNT}", fragmento)


class TestBultosRealesNoAgregados(unittest.TestCase):
    """La trampa conocida del Cubicador: peso_kg_u/peso_vol_u son SUM sobre
    toda la ficha, no medidas de un bulto. El laboratorio debe leer
    app_bultos directo, no esos agregados."""

    def test_lee_app_bultos_directo_no_los_campos_agregados(self):
        self.assertIn("BULTOS_TABLE", BULTOS_DOC)
        self.assertIn("product_id, bulto_num, largo, ancho, alto, peso", BULTOS_DOC)
        # El docstring de la funcion MENCIONA peso_kg_u/peso_vol_u a proposito
        # (explica la trampa que evita) -- lo que no debe existir es un ACCESO
        # real a esas claves (l.get(...)/l[...]), no la palabra en prosa.
        self.assertNotIn('.get("peso_kg_u")', BULTOS_DOC)
        self.assertNotIn('["peso_kg_u"]', BULTOS_DOC)
        self.assertNotIn('.get("peso_vol_u")', BULTOS_DOC)
        self.assertNotIn('["peso_vol_u"]', BULTOS_DOC)

    def test_excluye_lineas_zz_y_descuento(self):
        self.assertIn('l.get("es_zz")', BULTOS_DOC)
        self.assertIn('l.get("es_descuento")', BULTOS_DOC)

    def test_multiplica_por_bultos_equivalentes_no_por_cantidad(self):
        # Bug real conocido: usar 'cantidad' en vez de 'bultos_equivalentes'
        # duplica el peso en SKUs que se venden de a par.
        self.assertIn('bultos_equivalentes', BULTOS_DOC)
        i = BULTOS_DOC.index("veces_f = float(")
        fragmento = BULTOS_DOC[i:i + 200]
        self.assertNotIn('l.get("cantidad")', fragmento)

    def test_cada_bulto_conserva_sus_propias_medidas(self):
        self.assertIn('"largo":     float(fb["largo"]', BULTOS_DOC)
        self.assertIn('"ancho":     float(fb["ancho"]', BULTOS_DOC)
        self.assertIn('"alto":      float(fb["alto"]', BULTOS_DOC)


class TestNormalizacionMultibulto(unittest.TestCase):
    """Cada bulto real debe viajar como su PROPIO requestedPackageLineItem
    -- no colapsado en un groupPackageCount agregado."""

    def test_group_package_count_es_siempre_uno(self):
        self.assertIn('"groupPackageCount": 1,', NORMALIZAR)

    def test_asigna_sequence_number_por_bulto(self):
        self.assertIn("sequenceNumber", NORMALIZAR)
        self.assertIn("enumerate(bultos, start=1)", NORMALIZAR)

    def test_peso_minimo_medio_kilo_como_el_cotizador_existente(self):
        # Mismo piso que _fedex_calc_rate (max(round(peso,1), 0.5)) -- no se
        # inventa un criterio nuevo para el minimo facturable.
        self.assertIn("max(round(float(b.get(\"peso\") or 0), 1), 0.5)", NORMALIZAR)

    def test_dimensiones_son_opcionales_para_poder_aislar_su_efecto(self):
        self.assertIn("if con_dimensiones:", NORMALIZAR)
        self.assertIn('"units": "CM"', NORMALIZAR)


class TestVariantesDeControl(unittest.TestCase):
    """Las 7 variantes precargadas deben existir y cada CONTROL debe aislar
    exactamente una variable respecto de la variante A."""

    def test_existen_las_siete_variantes(self):
        for letra in "ABCDEFG":
            self.assertIn(f'"{letra}": {{', VARIANTES)

    def test_variante_a_no_impone_servicio_ni_carrier(self):
        i = VARIANTES.index('"A": {')
        fragmento = VARIANTES[i:i + 400]
        self.assertIn('"service_type": None, "carrier_codes": None', fragmento)
        self.assertIn('"ACCOUNT", "LIST"', fragmento)

    def test_variante_d_es_control_de_tarifa_publica(self):
        i = VARIANTES.index('"D": {')
        fragmento = VARIANTES[i:i + 300]
        self.assertIn('"rate_request_types": ["LIST"]', fragmento)

    def test_variante_e_reproduce_el_bug_de_produccion(self):
        i = VARIANTES.index('"E": {')
        fragmento = VARIANTES[i:i + 400]
        self.assertIn('"forzar_bulto_unico": True', fragmento)
        self.assertIn('["FDXE"]', fragmento)

    def test_variante_g_reproduce_el_reparto_parejo_de_ship_hoy(self):
        i = VARIANTES.index('"G": {')
        fragmento = VARIANTES[i:i + 450]
        self.assertIn('"forzar_reparto_parejo": True', fragmento)

    def test_variante_todas_corre_las_siete(self):
        self.assertIn('"TODAS"', ENDPOINT)
        self.assertIn("claves = list(_FEDEX_LAB_VARIANTES.keys())", ENDPOINT)


class TestDireccionNuncaVaVacia(unittest.TestCase):
    """Hallazgo de la investigacion: el codigo de hoy manda streetLines
    [""] para el destinatario, y el propio _fedex_split_address documenta
    que FedEx rechaza eso. El laboratorio usa el helper, nunca un array
    vacio a mano."""

    def test_usa_fedex_split_address_para_ambas_direcciones(self):
        self.assertIn("calle_dest  = _fedex_split_address(", ENDPOINT)
        self.assertIn("calle_org  = _fedex_split_address(", ENDPOINT)

    def test_no_arma_streetlines_vacio_a_mano(self):
        self.assertNotIn('"streetLines": [""]', ENDPOINT)
        self.assertNotIn("'streetLines': ['']", ENDPOINT)


if __name__ == "__main__":
    unittest.main(verbosity=2)

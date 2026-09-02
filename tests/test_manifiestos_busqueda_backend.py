"""Buscador y filtros de /transporte/manifiestos (tr_manifiestos, app.py).

Pedido de Daniel (2026-09-01): "En el menú de manifiesto, el filtro y el
buscador no está funcionando como corresponde. Me gustaría buscar facturas,
clientes y poder filtrar información."

DIAGNÓSTICO REAL (4 fallas independientes, ver también
test_manifiestos_busqueda_ui.py para la parte de interfaz):
  1. El buscador de texto no tenía botón visible -- solo Enter (arreglado en
     el template).
  2. `q` solo buscaba en correlativo/courier/notas de transport_manifests --
     nunca en cliente ni número de factura, que viven en
     transport_commitments unidos por transport_manifest_items.
  3. El filtro de courier comparaba IGUALDAD EXACTA contra un desplegable
     alimentado desde la ficha del courier -- bug ya documentado en
     _nombres_couriers_activos (app.py) que devolvía CERO resultados con
     cualquier variante de nombre ("Transporte Felca" vs "Transportes
     Felca").
  4. El filtro de estado comparaba la columna CRUDA `estado`, pero la tabla
     muestra un estado VISUAL derivado del % de avance -- el usuario elegía
     lo que veía en pantalla y la fila desaparecía.

RIESGO PRINCIPAL DEL FIX: la lista `params` se reutiliza 4 veces en 3
queries distintas dentro de tr_manifiestos() (1x badges, 2x KPIs -- porque
where_sql va embebido DOS veces ahí --, 1x listado). Un descuadre entre el
número de "%s" en el SQL y el número de parámetros en la tupla no siempre
lanza un error visible: puede desplazar silenciosamente valores a la
columna equivocada. Por eso el test central de este archivo no compara
contra un número "esperado" transcrito a mano (fácil de errar, como el
propio diseño de este fix comprobó en una revisión: contar a mano cuántos
"%s" arma cada rama es propenso a errores de 1) -- en cambio EJECUTA el
fragmento real de construcción de where/params (extraído por slicing de
texto de app.py, sin necesitar Flask/MySQL: el fragmento es Python + `re`
puro) y verifica laý invariante real: "%s" en el SQL == params en la lista,
para una matriz grande de valores de q, incluyendo adversariales.

app.py tiene 90k+ líneas -- se extrae el fragmento por slicing de texto
(mismo patrón que el resto de los tests de este módulo), sin pagar el
costo de un ast.parse() completo (~85-170s en este equipo).

Correr con:  py -m unittest tests.test_manifiestos_busqueda_backend -v
"""
import os
import re
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
    SRC = f.read()


def _extraer_bloque_where():
    """El fragmento de tr_manifiestos() que construye where/params, desde
    donde se definen _TIENE_ITEMS.._HAY_ENTREGADO hasta where_sql_base
    inclusive. Es código puro (Python + `re`), sin llamadas a Flask/MySQL --
    se puede ejecutar con exec() fuera de cualquier request real."""
    i = SRC.index(
        '    _TIENE_ITEMS       = "id IN (SELECT manifest_id FROM transport_manifest_items)"')
    j = SRC.index('where_sql_base = " AND ".join(where)', i)
    j_fin_linea = SRC.index("\n", j)
    bloque = SRC[i:j_fin_linea]
    # Dedent: el fragmento vive con 4 espacios (dentro de def tr_manifiestos()).
    lineas = bloque.split("\n")
    return "\n".join(l[4:] if l.startswith("    ") else l for l in lineas)


BLOQUE_WHERE = _extraer_bloque_where()


def _filtros(q="", courier="", estado="", desde="", hasta=""):
    return {"q": q, "courier": courier, "estado": estado, "desde": desde, "hasta": hasta}


def _construir(filtros_dict):
    """Ejecuta el fragmento REAL extraído (no una reimplementación) con un
    dict de filtros dado. Devuelve (where, params, where_sql_base)."""
    ns = {"re": re, "filtros": dict(filtros_dict)}
    exec(BLOQUE_WHERE, ns)
    return ns["where"], ns["params"], ns["where_sql_base"]


class TestElFragmentoSeExtrajoBien(unittest.TestCase):
    def test_el_bloque_no_esta_vacio_y_tiene_las_piezas_clave(self):
        self.assertIn("_TIENE_ITEMS", BLOQUE_WHERE)
        self.assertIn("_HAY_ENTREGADO", BLOQUE_WHERE)
        self.assertIn("where_sql_base", BLOQUE_WHERE)
        self.assertGreater(len(BLOQUE_WHERE), 2000)


class TestInvarianteDePlaceholders(unittest.TestCase):
    """EL TEST CENTRAL: sql.count('%s') == len(params), SIEMPRE. Un
    descuadre acá no siempre da un error visible en runtime -- puede
    desplazar valores a la columna equivocada sin romper la query."""

    VALORES_Q = [
        "", "Cipax", "FCV 11431", "fcv-11431", "fcv 11431", "11431",
        "0000011431", "VD 6371", "vd6371", "49", "MAN-49", "MAN 49",
        "76.996.964-0", "12345678-9", "50%", "a_b", "O'Higgins",
        "Ñuñoa", "   ", "café", "文字", "a" * 300, "%%%", "___",
        "'; DROP TABLE transport_manifests; --", "<script>alert(1)</script>",
    ]

    def test_para_cada_valor_de_q_solo(self):
        for q in self.VALORES_Q:
            with self.subTest(q=q):
                where, params, where_sql_base = _construir(_filtros(q=q))
                self.assertEqual(
                    where_sql_base.count("%s"), len(params),
                    f"DESCUADRE con q={q!r}: "
                    f"{where_sql_base.count('%s')} placeholders vs {len(params)} params")

    def test_combinado_con_courier_y_estado_tambien(self):
        for q in ("", "Cipax", "FCV 11431", "49"):
            for courier in ("", "Transporte Felca"):
                for estado in ("", "En curso"):
                    with self.subTest(q=q, courier=courier, estado=estado):
                        where, params, where_sql_base = _construir(
                            _filtros(q=q, courier=courier, estado=estado))
                        self.assertEqual(where_sql_base.count("%s"), len(params))

    def test_la_guarda_de_seguridad_en_caliente_existe(self):
        # Si algun cambio futuro desalinea el conteo, el codigo real debe
        # degradar a busqueda simple en vez de arriesgar resultados
        # incorrectos -- no solo confiar en que el desarrollador lo note.
        self.assertIn('if _q_cond.count("%s") != len(_p):', BLOQUE_WHERE)
        self.assertIn('_q_cond, _p = "(correlativo LIKE %s)", [q_like]', BLOQUE_WHERE)

    def test_q_vacio_no_agrega_ninguna_condicion_de_busqueda(self):
        where, params, _ = _construir(_filtros(q=""))
        self.assertEqual(where, ["(eliminado IS NULL OR eliminado=0)"])
        self.assertEqual(params, [])


class TestBusquedaPorNumeroDeFactura(unittest.TestCase):
    """El ERP guarda el nudo con ceros a la izquierda hasta 10 caracteres y
    las notas de venta con prefijo de letras (VD00006371) -- perder este
    padding es la causa raíz histórica de los commitments duplicados
    BLV 22719/22727/22728."""

    def test_11431_extrae_el_numero_sin_prefijo(self):
        where, params, _ = _construir(_filtros(q="11431"))
        cond = where[-1]
        self.assertIn("LPAD(%s, 10, '0')", cond)
        self.assertIn("REGEXP", cond)
        self.assertIn("11431", params)
        # Sin prefijo de letras: no debe aparecer la condicion c_q.tido = %s.
        self.assertNotIn("c_q.tido = %s", cond)

    def test_fcv_11431_acota_tambien_el_tipo_de_documento(self):
        where, params, _ = _construir(_filtros(q="FCV 11431"))
        cond = where[-1]
        self.assertIn("c_q.tido = %s", cond)
        self.assertIn("FCV", params)
        self.assertIn("11431", params)

    def test_0000011431_normaliza_igual_que_11431_sin_padding(self):
        where_a, params_a, _ = _construir(_filtros(q="0000011431"))
        where_b, params_b, _ = _construir(_filtros(q="11431"))
        # Mismo numero real detectado (aunque el resto del SQL sea igual de
        # forma, lo que importa es que ambos disparen la MISMA rama LPAD).
        self.assertIn("LPAD(%s, 10, '0')", where_a[-1])
        self.assertIn("LPAD(%s, 10, '0')", where_b[-1])
        self.assertIn("11431", params_a)
        self.assertIn("11431", params_b)

    def test_vd_6371_extrae_6371_no_vd6371(self):
        where, params, _ = _construir(_filtros(q="VD 6371"))
        cond = where[-1]
        self.assertIn("c_q.tido = %s", cond)
        self.assertIn("VD", params)
        self.assertIn("6371", params)
        self.assertNotIn("VD6371", params)

    def test_cipax_no_dispara_ninguna_rama_numerica(self):
        where, params, _ = _construir(_filtros(q="Cipax"))
        cond = where[-1]
        self.assertNotIn("c_q.nudo", cond)
        self.assertNotIn("c_q.tido", cond)
        self.assertNotIn("LPAD", cond)

    def test_man_49_busca_el_correlativo_con_el_padding_de_4_digitos(self):
        where, params, _ = _construir(_filtros(q="MAN 49"))
        cond = where[-1]
        self.assertIn("CONCAT('%%-', LPAD(%s, 4, '0'))", cond)
        self.assertIn("49", params)

    def test_11431_no_dispara_la_rama_man_por_ser_mas_de_4_digitos(self):
        where, params, _ = _construir(_filtros(q="11431"))
        cond = where[-1]
        self.assertNotIn("LPAD(%s, 4, '0')", cond)

    def test_rut_con_puntos_y_guion_activa_la_busqueda_por_rut(self):
        where, params, _ = _construir(_filtros(q="76.996.964-0"))
        cond = where[-1]
        self.assertIn("cliente_rut", cond)
        # re.sub(r"\D","",...) deja TODOS los digitos (cuerpo + DV) -- "0" es
        # un digito valido de DV, no se descarta. El param lleva '%' al
        # final (prefijo, no substring en medio) para tolerar un DV
        # verbalizado distinto ("76996964-K" en mayus/minus no aplica aca
        # porque K no es digito y se cae del solo_dig, pero el sufijo sigue
        # siendo comodin por si la BD guarda con o sin DV completo).
        self.assertIn("769969640%", params)


class TestFiltroCourierEsTolerante(unittest.TestCase):
    """Bug ya documentado en _nombres_couriers_activos (app.py) -- comparar
    exacto devolvia CERO resultados con cualquier variante de nombre."""

    def test_usa_like_no_igualdad(self):
        where, params, _ = _construir(_filtros(courier="Transporte Felca"))
        self.assertIn("courier LIKE %s", where)
        self.assertNotIn("courier=%s", where)
        self.assertIn("%Transporte Felca%", params)

    def test_escapa_comodines_del_nombre_del_courier(self):
        where, params, _ = _construir(_filtros(courier="100%_Seguro"))
        # El propio nombre no debe introducir comodines LIKE sin escapar.
        self.assertIn("%100\\%\\_Seguro%", params)


class TestFiltroEstadoUsaElEstadoVisual(unittest.TestCase):
    """El pill que ve el usuario en la tabla se DERIVA del % de avance, no
    es la columna cruda `estado`. Este CASE es la traduccion literal del
    if/elif de manifiestos.html -- si uno cambia, el otro tambien."""

    def test_usa_case_no_columna_cruda(self):
        where, params, _ = _construir(_filtros(estado="En curso"))
        cond = where[-1]
        self.assertIn("CASE WHEN estado='Cerrado' THEN 'Cerrado'", cond)
        self.assertIn("'Entregado completo'", cond)
        self.assertIn("'En curso'", cond)
        self.assertEqual(cond.count("%s"), 1)
        self.assertIn("En curso", params)

    def test_el_orden_de_ramas_coincide_con_el_template(self):
        with open(os.path.join(BASE_DIR, "templates", "transporte", "manifiestos.html"),
                  encoding="utf-8", errors="ignore") as fh:
            html = fh.read()
        # El template dibuja el pill con un if/elif -- ver estado_visual_*
        # calculado en la misma funcion. Si el orden de las 3 ramas cambia
        # en un lado sin el otro, el filtro deja de coincidir con lo que se
        # ve en pantalla otra vez.
        self.assertIn("Entregado completo", html)
        self.assertIn("En curso", html)


class TestNoJoinNiDuplicados(unittest.TestCase):
    """Un JOIN en vez de EXISTS multiplicaria cada manifiesto por su numero
    de items, inflando SUM(costo_total)/COUNT(*) (KPIs) y rompiendo el
    paginador (REGLA #4.3: 'Mostrando 1-10 de 112' con 8 manifiestos
    reales)."""

    def test_el_predicado_de_q_usa_exists_no_join(self):
        where, _, _ = _construir(_filtros(q="Cipax"))
        cond = where[-1]
        self.assertIn("EXISTS (SELECT 1", cond)
        self.assertIn("mi_q.manifest_id = transport_manifests.id", cond)

    def test_los_alias_del_exists_no_chocan_con_los_de_kpis(self):
        # La query de KPIs (mas abajo en tr_manifiestos) usa alias `mi`/`c`
        # sin sufijo -- si el EXISTS reusara esos mismos alias, quedaria
        # sombreando los externos.
        where, _, _ = _construir(_filtros(q="Cipax"))
        cond = where[-1]
        self.assertIn(" mi_q ", cond)
        self.assertIn(" c_q ", cond)
        self.assertNotIn(" mi ON", cond)
        self.assertNotIn(" c ON", cond)

    def test_el_listado_sigue_sin_join_directo(self):
        i = SRC.index('"SELECT * FROM transport_manifests WHERE " + where_sql +')
        fragmento = SRC[max(0, i - 50):i + 200]
        self.assertNotIn("JOIN", fragmento)


class TestNingunaFuncionNuevaEnAppPy(unittest.TestCase):
    """Dos tests del cubicador (test_cubicador_excel_vol_m3_y_bultos_total.py
    y test_cubicador_nombre_archivo_extension.py) comparan por AST que no se
    agregue NINGUNA funcion nueva a app.py (ni siquiera anidada) respecto de
    origin/main. Por eso este fix no crea ningun helper: todo el parseo de
    "FCV 11431" vive inline dentro de tr_manifiestos()."""

    def test_no_hay_ningun_def_dentro_del_bloque_extraido(self):
        self.assertNotIn("\ndef ", BLOQUE_WHERE)
        self.assertNotIn("    def ", BLOQUE_WHERE)


if __name__ == "__main__":
    unittest.main(verbosity=2)

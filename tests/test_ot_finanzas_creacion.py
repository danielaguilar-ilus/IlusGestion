"""Quién escribe la plata de una OT -- modelo único "Cobré − Me cobraron = Queda" (+ Valorizado aparte).
Daniel, 2026-10-07.

Cada número nace en su casillero: lo cobrado en zz_monto/zz_envio_monto, lo que nos cobra el técnico en
costo_proveedor/costo_despacho y cuánto VALE un trabajo que no se cobra en valorizado_clp. `costo` ("Precio al
cliente") ya no se escribe al crear ni desde estos flujos; asociar/quitar documentos solo lo mantiene al día en
OT antiguas que ya lo tenían (revisión adversarial del mismo día).

Sin BD ni Flask: las funciones se extraen de app.py con ast (mismo patrón que tests/test_ot_finanzas_modelo.py)
y la vista previa del asistente se corre en node contra static/ot_finanzas.js (se salta si no hay node).
Correr con:  py -m unittest tests.test_ot_finanzas_creacion
"""
import ast
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNCS = ("_ot_es_interna", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp", "_ot_finanzas",
         "_ot_fin_valorizado_fuente", "_ot_fin_reparto_creacion", "_ot_validar_normalizar_finanzas",
         "_ot2_finanzas_estado", "_anexo_ot_en_garantia", "_pl_cobertura_contrato", "_ot_fin_sql_contrato_real")
CONSTS = ("_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO", "_OT_FIN_COBERTURA_TXT",
          "_OT_FIN_UMBRAL_BAJO", "_OT_FIN_VALORIZADO_FUENTE_POR_ORIGEN", "_OT_FIN_VALORIZADO_FUENTES",
          "_OT2_CENTROS_COSTO", "_OT2_VALOR_ORIGENES", "_OT2_VALOR_ORIGENES_CON_MOTIVO", "_OT2_LINEA_ZZ",
          "_OT_FIN_SQL_CONTRATO_REAL", "_OT_FIN_SQL_COBRE_POSITIVO")

_AMB = None


def _amb():
    global _AMB
    if _AMB is None:
        import re
        _, arbol = _codigo_y_arbol()
        amb = {"json": json, "re": re, "print": lambda *a, **k: None}
        for nodo in arbol.body:
            if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                    and nodo.targets[0].id in CONSTS:
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
            if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
                nodo.decorator_list = []
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        faltan = [n for n in FUNCS + CONSTS if n not in amb]
        assert not faltan, faltan
        _AMB = amb
    return _AMB


def _fuente(nombre):
    with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
        app = fh.read().replace(chr(13) + chr(10), chr(10))
    i = app.index(f"def {nombre}(")
    j = app.find("\ndef ", i + 10)
    return app[i:j]


def N(fin, tipo="instalacion", interna=False, **kw):
    """_ot_validar_normalizar_finanzas sin tope de documentos (no se consulta el ERP en la prueba)."""
    amb = _amb()
    amb["_ot_zz_topes_reales"] = lambda *a, **k: {"excluidos": [], "documentos": [1],
                                                    "tope_servicio": 10 ** 9, "tope_despacho": 10 ** 9}
    return amb["_ot_validar_normalizar_finanzas"](fin, tipo, interna, **kw)


class TestReparto(unittest.TestCase):
    def R(self, cobertura, **c):
        return _amb()["_ot_fin_reparto_creacion"](c, cobertura)

    def test_si_se_cobra_nada_se_mueve(self):
        r = self.R("cobra", zz_codigo="ZZINSTALACION", zz_monto=150000, valor_origen="cotizacion")
        self.assertEqual(r, {"zz_codigo": "ZZINSTALACION", "zz_monto": 150000,
                             "valorizado_clp": None, "valorizado_fuente": None})

    def test_garantia_un_estimado_pasa_al_valorizado(self):
        r = self.R("garantia", zz_monto=200000, valor_origen="estimado")
        self.assertIsNone(r["zz_monto"], "un estimado nunca queda en el casillero de lo cobrado")
        self.assertEqual((r["valorizado_clp"], r["valorizado_fuente"]), (200000.0, "estimado"))

    def test_garantia_con_linea_real_del_documento_la_conserva_y_valoriza(self):
        r = self.R("garantia", zz_codigo="ZZINSTALACION", zz_monto=180000, valor_origen="zz")
        self.assertEqual(r["zz_monto"], 180000)
        self.assertEqual((r["valorizado_clp"], r["valorizado_fuente"]), (180000.0, "documento"))

    def test_zzretiro_nunca_es_valorizado(self):
        r = self.R("garantia", zz_codigo="ZZRETIRO", zz_monto=1, valor_origen="zz")
        self.assertIsNone(r["valorizado_clp"])
        self.assertEqual(r["zz_monto"], 1, "es una línea real del documento: queda como historia")

    def test_interno_su_valor_es_el_valorizado(self):
        r = self.R("interno", costo_interno=45000, valor_origen="interno")
        self.assertEqual((r["valorizado_clp"], r["valorizado_fuente"]), (45000.0, "interno"))
        r = self.R("interno", costo_interno=45000, valor_origen="manual")
        self.assertEqual(r["valorizado_fuente"], "a_mano")

    def test_valorizado_explicito_manda(self):
        r = self.R("garantia", zz_monto=200000, valor_origen="estimado", valorizado_clp=150000,
                   valorizado_fuente="cotizador")
        self.assertEqual((r["valorizado_clp"], r["valorizado_fuente"]), (150000, "cotizador"))
        self.assertIsNone(r["zz_monto"])

    def test_fuente_desconocida_es_a_mano(self):
        f = _amb()["_ot_fin_valorizado_fuente"]
        self.assertEqual(f(None, "inventada"), "a_mano")
        self.assertEqual(f("cotizacion"), "cotizador")
        self.assertEqual(f("zz", "contrato"), "contrato")


class TestCrearOT(unittest.TestCase):
    """_ot_validar_normalizar_finanzas: lo que se guarda al crear (ot2_api_crear, Tickets, Levantamiento,
    calendario/ficha)."""

    DOC = {"centro_costo": "sstt", "factura_tido": "FCV", "factura_nudo": "11439"}

    def test_cobra_con_documento_zz_queda_y_costo_ya_no_se_escribe(self):
        err, c = N(dict(self.DOC, zz_monto=150000, zz_envio_monto=30000, zz_envio_codigo="ZZENVIO",
                        valor_origen="zz", costo_proveedor=0))
        self.assertIsNone(err)
        self.assertEqual((c["zz_monto"], c["zz_envio_monto"]), (150000, 30000))
        self.assertIsNone(c["costo_cliente"], "`costo` ya no nace con la copia de zz+envío")
        self.assertEqual(c["costo_proveedor"], 0.0, "un 0 declarado es un dato")
        self.assertEqual(c["cobertura"], "cobra")
        self.assertIsNone(c["valorizado_clp"])

    def test_garantia_el_monto_va_al_valorizado(self):
        err, c = N({"centro_costo": "sstt", "garantia_aplica": True, "garantia_motivo": "falla de fábrica del motor",
                    "zz_monto": 200000, "valor_origen": "estimado"})
        self.assertIsNone(err)
        self.assertEqual(c["modalidad_cobro"], "garantia")
        self.assertIsNone(c["zz_monto"])
        self.assertEqual((c["valorizado_clp"], c["valorizado_fuente"]), (200000.0, "estimado"))
        self.assertIsNone(c["costo_cliente"])

    def test_garantia_sin_valorizar_se_puede_crear(self):
        err, c = N({"centro_costo": "sstt", "garantia_aplica": True, "garantia_motivo": "falla de fábrica del motor"})
        self.assertIsNone(err, "decisión de Daniel: el valorizado es sugerido, nunca obligatorio")
        self.assertIsNone(c["valorizado_clp"])

    def test_fuera_de_garantia_el_cobro_sigue_siendo_obligatorio(self):
        err, _ = N(dict(self.DOC))
        self.assertEqual(err["error_codigo"], "FINANZAS_SIN_MONTO")

    def test_valorizar_opcional_en_ot_que_se_cobra(self):
        err, c = N(dict(self.DOC, zz_monto=100000, valor_origen="zz", valorizado_clp="120000"))
        self.assertIsNone(err)
        self.assertEqual(c["zz_monto"], 100000)
        self.assertEqual((c["valorizado_clp"], c["valorizado_fuente"]), (120000.0, "a_mano"))

    def test_valorizado_invalido_o_cero(self):
        err, _ = N(dict(self.DOC, zz_monto=100000, valorizado_clp="abc"))
        self.assertEqual(err["error_codigo"], "VALORIZADO_INVALIDO")
        err, c = N(dict(self.DOC, zz_monto=100000, valorizado_clp=0))
        self.assertIsNone(err)
        self.assertIsNone(c["valorizado_clp"])

    def test_trabajo_interno_su_valor_va_al_valorizado(self):
        err, c = N({"costo_interno": 45000, "valor_origen": "interno"}, tipo="revision_interna", interna=True)
        self.assertIsNone(err)
        self.assertEqual(c["cobertura"], "interno")
        self.assertIsNone(c["costo_cliente"], "antes iba a `costo`")
        self.assertEqual((c["valorizado_clp"], c["valorizado_fuente"]), (45000.0, "interno"))

    def test_levantamiento_desde_ticket_se_guarda_sin_costo(self):
        # _mant_lev_crear_ot_core manda modalidad_forzada='sin_costo' (así lo guarda _ot_crear_visita_espejo).
        err, c = N(dict(self.DOC, zz_monto=60000, valor_origen="supuesto", zz_motivo_manual="tarifa habitual"),
                   tipo="levantamiento", modalidad_forzada="sin_costo")
        self.assertIsNone(err)
        self.assertEqual(c["cobertura"], "sin_costo")
        self.assertIsNone(c["zz_monto"])
        self.assertEqual((c["valorizado_clp"], c["valorizado_fuente"]), (60000.0, "supuesto"))

    def test_aplica_garantia_del_ticket_tambien_cuenta(self):
        err, c = N(dict(self.DOC, zz_monto=90000, valor_origen="cotizacion"), tipo="correctiva",
                   modalidad_forzada="garantia")
        self.assertIsNone(err)
        self.assertIsNone(c["zz_monto"])
        self.assertEqual(c["valorizado_fuente"], "cotizador")

    # Revisión adversarial 2026-10-07.
    def test_un_estimado_no_puede_ser_lo_cobrado(self):
        """Antes nacía con Cobré $0 y el cierre (SIN_VALORIZAR) la trababa sin salida clara."""
        err, _ = N(dict(self.DOC, zz_monto=80000, valor_origen="estimado"))
        self.assertEqual(err["error_codigo"], "FINANZAS_ESTIMADO_NO_ES_COBRO")
        self.assertIn("Editado a mano", err["error"])
        err, c = N(dict(self.DOC, zz_monto=80000, valor_origen="manual",
                        zz_motivo_manual="el documento no trae línea de servicio"))
        self.assertIsNone(err, "escrito a mano con su motivo sí es un cobro")
        self.assertEqual((c["zz_monto"], c["valor_origen"]), (80000, "manual"))

    def test_el_estimado_sigue_valiendo_donde_no_se_cobra(self):
        err, c = N({"centro_costo": "sstt", "garantia_aplica": True, "garantia_motivo": "falla de fábrica del motor",
                    "zz_monto": 80000, "valor_origen": "estimado"})
        self.assertIsNone(err)
        self.assertEqual(c["valorizado_clp"], 80000.0)
        err, _ = N(dict(self.DOC, zz_monto=60000, valor_origen="estimado"), tipo="levantamiento",
                   modalidad_forzada="sin_costo")
        self.assertIsNone(err, "el levantamiento nace sin costo: el estimado es su valorizado")

    def test_tipo_garantia_marcado_con_documento_no_nace_a_medias(self):
        err, _ = N(dict(self.DOC, zz_monto=80000, valor_origen="zz"), tipo="garantia")
        self.assertEqual(err["error_codigo"], "FINANZAS_TIPO_GARANTIA_CON_COBRO")
        err, c = N({"centro_costo": "sstt", "garantia_aplica": True, "garantia_motivo": "falla de fábrica del motor"},
                   tipo="garantia")
        self.assertIsNone(err)
        self.assertEqual(c["cobertura"], "garantia")
        err, _ = N(dict(self.DOC, zz_monto=80000, valor_origen="zz"), tipo="garantia", modalidad_forzada="garantia")
        self.assertIsNone(err, "Ticket con «aplica garantía»: la OT se guarda como garantía")


class TestEstadoFinanzas(unittest.TestCase):
    """_ot2_finanzas_estado (lo que la ficha y el monitor muestran como "faltan")."""

    def E(self, **v):
        base = {"centro_costo": "sstt", "modalidad_cobro": "pagado", "cubierto_por": "cliente", "tipo": "instalacion",
                "factura_nudo": "11439", "garantia_motivo": None}
        base.update(v)
        return _amb()["_ot2_finanzas_estado"](base)

    def test_un_estimado_ya_no_cuenta_como_cobro(self):
        ok, faltan = self.E(zz_monto=80000, valor_origen="estimado", costo=80000)
        self.assertFalse(ok)
        self.assertTrue(any("cuánto se cobra" in f for f in faltan))

    def test_la_linea_del_documento_si(self):
        self.assertEqual(self.E(zz_monto=80000, valor_origen="zz"), (True, []))

    def test_ot_antigua_con_precio_al_cliente_sigue_valorizada(self):
        self.assertEqual(self.E(zz_monto=None, costo=120000), (True, []))

    def test_garantia_por_cubierto_por_no_pide_documento(self):
        ok, faltan = self.E(cubierto_por="garantia", factura_nudo=None, garantia_motivo="falla de fábrica")
        self.assertEqual((ok, faltan), (True, []))

    def test_cortesia_no_pide_documento_ni_monto(self):
        self.assertEqual(self.E(modalidad_cobro="sin_costo", factura_nudo=None, zz_monto=None), (True, []))

    def test_contrato_real_cuando_el_llamador_trae_la_bandera(self):
        ok, faltan = self.E(tipo="preventiva", cubierto_por="contrato", contrato_real=1, factura_nudo=None,
                            garantia_motivo="Plan Anual, contrato 2026")
        self.assertEqual((ok, faltan), (True, []))
        ok, faltan = self.E(tipo="preventiva", cubierto_por="contrato", contrato_real=0, factura_nudo=None)
        self.assertFalse(ok, "sin contrato real se cobra: pide documento")

    def test_sin_la_bandera_se_conserva_el_criterio_anterior_de_contrato(self):
        ok, faltan = self.E(cubierto_por="contrato", factura_nudo=None, garantia_motivo=None)
        self.assertEqual(faltan, ["motivo/contrato que cubre esta visita"])

    def test_interno_lee_el_valorizado(self):
        b = {"centro_costo": "sstt", "modalidad_cobro": "interno", "tipo": "revision_interna"}
        self.assertEqual(_amb()["_ot2_finanzas_estado"](dict(b, valorizado_clp=45000, costo=None)), (True, []))
        ok, faltan = _amb()["_ot2_finanzas_estado"](dict(b, valorizado_clp=None, costo=0))
        self.assertFalse(ok)
        self.assertEqual(_amb()["_ot2_finanzas_estado"](dict(b, costo=0)), (True, []),
                         "sin valorizado_clp en la fila no se puede juzgar: no inventa un 'falta'")


class TestAnexoEnGarantia(unittest.TestCase):
    def test_lee_la_cobertura_y_no_una_columna_inexistente(self):
        amb = _amb()
        consultas = []

        def fetch(sql, params):
            consultas.append(sql)
            return {"modalidad_cobro": "pagado", "cubierto_por": "garantia", "tipo": "correctiva"}
        amb["mysql_fetchone"] = fetch
        self.assertTrue(amb["_anexo_ot_en_garantia"](10))
        self.assertNotIn("garantia_aplica", consultas[0])
        amb["mysql_fetchone"] = lambda sql, params: {"modalidad_cobro": "pagado", "cubierto_por": "cliente",
                                                     "tipo": "correctiva"}
        self.assertFalse(amb["_anexo_ot_en_garantia"](10))


class TestPlanAnual(unittest.TestCase):
    def P(self, contrato_id, nombre, estado, valor, real_en_vigor=None):
        """contrato_id/nombre/estado: el que devuelve _intel_contrato_id. real_en_vigor: (id, nombre) de OTRO
        contrato real en vigor del cliente, o None."""
        amb = _amb()
        amb["_intel_contrato_id"] = lambda cid: contrato_id

        def fetch(sql, params=()):
            if "valor_mantencion_clp" in sql:
                return {"valor_mantencion_clp": valor}
            if "SELECT nombre, estado FROM mant_contratos WHERE id=" in sql:
                return {"nombre": nombre, "estado": estado}
            if "SELECT id, nombre FROM mant_contratos WHERE cliente_id=" in sql:
                return ({"id": real_en_vigor[0], "nombre": real_en_vigor[1]} if real_en_vigor else None)
            return None
        amb["mysql_fetchone"] = fetch
        return amb["_pl_cobertura_contrato"](5)

    def test_contrato_real_no_se_cobra_y_valoriza(self):
        r = self.P(7, "Contrato anual 2026", "vigente", 60000)
        self.assertEqual((r["cubierto_por"], r["contrato_id"]), ("contrato", 7))
        self.assertEqual((r["valorizado_clp"], r["valorizado_fuente"]), (60000.0, "contrato"))
        self.assertIsNone(r["zz_monto"])
        self.assertEqual(self.P(7, "Contrato anual 2026", "por_vencer", 60000)["cubierto_por"], "contrato")

    def test_sin_contrato_el_precio_se_cobra(self):
        r = self.P(None, "", None, 60000)
        self.assertEqual(r["cubierto_por"], "cliente")
        self.assertEqual((r["zz_monto"], r["valor_origen"]), (60000, "contrato"))
        self.assertIsNone(r["valorizado_clp"])

    def test_el_contenedor_de_documentos_no_es_contrato(self):
        r = self.P(9, "Contenedor de documentos", "vigente", 60000)
        self.assertEqual(r["cubierto_por"], "cliente")
        self.assertEqual(r["zz_monto"], 60000)
        self.assertEqual(r["contrato_id"], 9, "amarrarla al ficticio no la vuelve contrato (la regla lo excluye)")
        r = self.P(9, "Contenedor de documentos", "vigente", 60000, real_en_vigor=(4, "Contrato 2026"))
        self.assertEqual((r["cubierto_por"], r["contrato_id"]), ("contrato", 4))

    def test_contrato_vencido_se_cobra_y_no_se_amarra(self):
        """Revisión adversarial 2026-10-07: _intel_contrato_id cae al contrato más reciente aunque esté vencido,
        y la regla de lectura cuenta un contrato real vencido si la OT apunta a él."""
        r = self.P(7, "Contrato 2024", "vencido", 60000)
        self.assertEqual(r["cubierto_por"], "cliente")
        self.assertIsNone(r["contrato_id"], "si apuntara al vencido, la OT se leería 'contrato: no se cobra'")
        self.assertEqual((r["zz_monto"], r["valor_origen"]), (60000, "contrato"))
        self.assertIsNone(r["valorizado_clp"])
        r = self.P(7, "Contrato 2024", "vencido", 60000, real_en_vigor=(8, "Contrato 2026"))
        self.assertEqual((r["cubierto_por"], r["contrato_id"]), ("contrato", 8))
        self.assertIn("Contrato 2026", r["motivo"])

    def test_los_insert_ya_no_escriben_costo(self):
        for nombre in ("mant_planificador_generar_ots", "mant_intel_accion"):
            cuerpo = _fuente(nombre)
            self.assertIn("valorizado_clp, valorizado_fuente", cuerpo, nombre)
            self.assertNotIn("garantia_motivo, costo,", cuerpo, nombre)
            self.assertNotIn('_pl_cob["costo"]', cuerpo, nombre)
            self.assertNotIn('_rv_cob["costo"]', cuerpo, nombre)


class TestQuienYaNoEscribeCosto(unittest.TestCase):
    def test_declarar_cobertura_va_al_valorizado_y_es_opcional(self):
        c = _fuente("mant_ot_declarar_cobertura")
        self.assertNotIn('sets.append("costo=%s")', c)
        self.assertIn('sets += ["valorizado_clp=%s", "valorizado_fuente=%s"]', c)
        self.assertNotIn("return jsonify", c[c.index("_monto_raw = d.get"):c.index("v = mysql_fetchone(")],
                         "el monto ya no se exige")
        self.assertIn("_mant_log(", c, "la constancia en mant_logs se conserva")

    def test_asociar_factura_no_rellena_costo(self):
        self.assertNotIn("costo=COALESCE", _fuente("mant_ot_asociar_factura"))

    def test_put_no_escribe_costo_y_guarda_el_cero(self):
        import re
        c = _fuente("mant_visita_update")
        allowed = c[c.index("allowed = ["):c.index("_ESTADOS_PROTEGIDOS_PUT")]
        self.assertIsNone(re.search(r'^\s*"costo"\s*,', allowed, re.M), "`costo` salió de la lista del PUT")
        self.assertIn('"contrato_id",', allowed)
        self.assertNotIn('float(d.get("costo_proveedor") or 0)) or None', c)
        self.assertIn('for _campo_k in ("costo_proveedor", "costo_despacho"):', c)

    def test_put_un_cobro_escrito_sobre_un_estimado_queda_manual(self):
        """Revisión adversarial 2026-10-07: el rótulo 'estimado' se quedaba pegado al número nuevo."""
        c = _fuente("mant_visita_update")
        bloque = c[c.index("_origen_relabel = None"):c.index("tecnico_prev = None")]
        self.assertIn("in _OT_FIN_ORIGENES_NO_COBRO", bloque)
        self.assertIn('sets.append("valor_origen=%s")', bloque)
        self.assertIn('vals.append("manual")', bloque)
        allowed = c[c.index("allowed = ["):c.index("_ESTADOS_PROTEGIDOS_PUT")]
        self.assertNotIn('"valor_origen"', allowed, "el origen no se escribe libre por el PUT")

    def test_visita_multiequipo_estima_en_el_valorizado(self):
        c = _fuente("mant_visita_multi")
        self.assertIn("valorizado_clp,valorizado_fuente,modalidad_cobro", c)
        self.assertIn('("estimado" if costo else None)', c)

    def test_candados_de_cierre_usan_la_regla_unica(self):
        c = _fuente("mant_ot_aprobar_cierre")
        self.assertIn("_cob_cierre = _ot_cobertura(v)", c)
        self.assertIn('_ot_finanzas(v)["cobre"]["total"] > 0', c)
        self.assertIn('_ot_fin_sql_contrato_real("v")', c)
        self.assertNotIn('or float(v.get("costo") or 0) > 0)):', c, "`costo` > 0 ya no basta para cerrar")
        # ANEXO_DESACTUALIZADO sigue con su criterio (es la plata del PROVEEDOR, no del cliente).
        self.assertIn('_mod_cobro not in ("garantia", "sin_costo"):', c)


class TestSqlCobrePositivo(unittest.TestCase):
    """_OT_FIN_SQL_COBRE_POSITIVO (el booleano `_fin_valorizada` del Monitor) da lo mismo que
    _ot_finanzas(v)["cobre"]["total"] > 0 en una OT que se cobra. Se corre en sqlite (mismas funciones SQL)."""

    CASOS = [
        dict(zz_monto=150000, zz_envio_monto=30000, valor_origen="zz", zz_codigo="ZZINSTALACION"),
        dict(zz_monto=80000, valor_origen="estimado", costo=80000),                 # estimado: no es cobro
        dict(zz_monto=80000, valor_origen=" Estimado ", zz_envio_monto=20000),      # solo cuenta el despacho
        dict(zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz"),                  # OT-201: no es servicio
        dict(zz_monto=1, zz_codigo="zzretiro ", valor_origen="zz", costo=200000),   # ZZRETIRO + precio antiguo
        dict(zz_monto=None, costo=120000),                                          # OT antigua
        dict(zz_monto=0, costo=90000, valor_origen="zz"),                           # zz en 0 manda sobre costo
        dict(zz_monto=0, zz_envio_monto=0, valor_origen="manual"),                  # cobro declarado en $0
        dict(zz_monto=None, zz_envio_monto=15000),
        dict(zz_monto=None, costo=None),
        dict(zz_monto=50000, valor_origen="manual"),
        dict(zz_monto=50000, valor_origen=None, zz_codigo=None),
        dict(zz_monto=70000, valor_origen="interno"),
        dict(zz_monto=40000, valor_origen="supuesto", costo=0),
    ]

    def test_misma_respuesta_que_la_regla(self):
        amb = _amb()
        expr = amb["_OT_FIN_SQL_COBRE_POSITIVO"]
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, zz_monto INTEGER, zz_envio_monto INTEGER, "
                   "costo REAL, valor_origen TEXT, zz_codigo TEXT)")
        for i, caso in enumerate(self.CASOS, 1):
            fila = dict(zz_monto=None, zz_envio_monto=None, costo=None, valor_origen=None, zz_codigo=None)
            fila.update(caso)
            db.execute("INSERT INTO mant_visitas VALUES (?,?,?,?,?,?)",
                       (i, fila["zz_monto"], fila["zz_envio_monto"], fila["costo"], fila["valor_origen"],
                        fila["zz_codigo"]))
            sql_dice = bool(db.execute("SELECT " + expr + " FROM mant_visitas v WHERE v.id=?", (i,)).fetchone()[0])
            v = dict(fila, modalidad_cobro="pagado", cubierto_por="cliente", tipo="instalacion", cliente_id=3)
            fin = amb["_ot_finanzas"](v)
            self.assertEqual(fin["cobertura"], "cobra")
            self.assertEqual(sql_dice, fin["cobre"]["total"] > 0, caso)

    def test_el_monitor_la_usa(self):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            app = fh.read()
        self.assertIn('"       " + _OT_FIN_SQL_COBRE_POSITIVO + " AS _fin_valorizada, "', app)
        self.assertNotIn("OR COALESCE(v.costo,0) > 0) AS _fin_valorizada", app)


class TestDocumentosSinCosto(unittest.TestCase):
    """Asociar/quitar un documento (multidocumento) ya no le crea `costo` a una OT nueva. Se corren los UPDATE
    reales de app.py en sqlite."""

    @staticmethod
    def _updates(nombre):
        _, arbol = _codigo_y_arbol()
        fn = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == nombre)
        out = []
        for n in ast.walk(fn):
            if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "mysql_execute"
                    and n.args and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str)
                    and n.args[0].value.startswith("UPDATE mant_visitas SET") and "zz_monto" in n.args[0].value):
                out.append(n.args[0].value.replace("%s", "?"))
        return out

    def _db(self, **fila):
        db = sqlite3.connect(":memory:")
        db.create_function("GREATEST", 2, max)
        db.execute("CREATE TABLE mant_visitas (id INTEGER PRIMARY KEY, zz_monto INTEGER, zz_envio_monto INTEGER, "
                   "costo REAL, zz_codigo TEXT, valor_origen TEXT)")
        base = dict(zz_monto=None, zz_envio_monto=None, costo=None, zz_codigo=None, valor_origen=None)
        base.update(fila)
        db.execute("INSERT INTO mant_visitas VALUES (1,?,?,?,?,?)",
                   (base["zz_monto"], base["zz_envio_monto"], base["costo"], base["zz_codigo"], base["valor_origen"]))
        return db

    @staticmethod
    def _fila(db):
        r = db.execute("SELECT zz_monto, zz_envio_monto, costo, zz_codigo, valor_origen FROM mant_visitas").fetchone()
        return dict(zip(("zz_monto", "zz_envio_monto", "costo", "zz_codigo", "valor_origen"), r))

    def test_segundo_documento_en_ot_nueva(self):
        """Escenario del revisor: servicio 150k + 50k, despacho 30k. Antes `costo` quedaba en 50k y salía el
        aviso falso «el Precio al cliente ($50.000) no coincide con lo cobrado ($230.000)»."""
        sql = [s for s in self._updates("ot2_api_documentos_agregar") if "COALESCE(zz_monto,0)+" in s]
        self.assertEqual(len(sql), 1)
        db = self._db(zz_monto=150000, zz_envio_monto=30000, zz_codigo="ZZINSTALACION", valor_origen="zz")
        db.execute(sql[0], (50000, 0, 50000, "ZZINSTALACION", 1))
        f = self._fila(db)
        self.assertEqual((f["zz_monto"], f["zz_envio_monto"], f["costo"]), (200000, 30000, None))
        fin = _amb()["_ot_finanzas"](dict(f, modalidad_cobro="pagado", cubierto_por="cliente", tipo="instalacion",
                                          cliente_id=3))
        self.assertEqual(fin["cobre"]["total"], 230000)
        self.assertFalse([a for a in fin["avisos"] if "no coincide" in a], fin["avisos"])

    def test_ot_antigua_sigue_cuadrando(self):
        sql = [s for s in self._updates("ot2_api_documentos_agregar") if "COALESCE(zz_monto,0)+" in s][0]
        db = self._db(zz_monto=150000, zz_envio_monto=30000, costo=180000, valor_origen="zz")
        db.execute(sql, (50000, 0, 50000, "ZZINSTALACION", 1))
        self.assertEqual(self._fila(db)["costo"], 230000)

    def test_primer_documento(self):
        sql = [s for s in self._updates("ot2_api_documentos_agregar") if "valor_origen='zz'" in s]
        self.assertEqual(len(sql), 1)
        db = self._db(costo=None)
        db.execute(sql[0], (100000, 20000, 120000, "ZZMANTENCION", 1))
        f = self._fila(db)
        self.assertEqual((f["zz_monto"], f["zz_envio_monto"], f["costo"], f["valor_origen"]),
                         (100000, 20000, None, "zz"))
        db = self._db(costo=0)
        db.execute(sql[0], (100000, 20000, 120000, "ZZMANTENCION", 1))
        self.assertEqual(self._fila(db)["costo"], 0, "el 0 que deja _mant_visita_crear_core no se toca")
        db = self._db(costo=90000, valor_origen="estimado")
        db.execute(sql[0], (100000, 20000, 120000, "ZZMANTENCION", 1))
        self.assertEqual(self._fila(db)["costo"], 120000, "OT antigua: como siempre, el documento fija el total")

    def test_quitar_documento(self):
        sql = self._updates("ot2_api_documentos_quitar")
        self.assertEqual(len(sql), 1)
        db = self._db(zz_monto=200000, zz_envio_monto=30000, costo=None)
        db.execute(sql[0], (50000, 0, 50000, 1))
        self.assertEqual(self._fila(db)["costo"], None, "antes un `costo` vacío quedaba en 0")
        db = self._db(zz_monto=200000, zz_envio_monto=30000, costo=230000)
        db.execute(sql[0], (50000, 0, 50000, 1))
        self.assertEqual(self._fila(db)["costo"], 180000)


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestVistaPreviaAsistente(unittest.TestCase):
    """La vista previa del paso Costos (templates/ot2/_modal_crear.html) usa el espejo de la regla única."""

    def test_vista_previa(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as fh:
            html = fh.read().replace(chr(13) + chr(10), chr(10))
        i = html.index("  var _O2M_VALORIZADO_FUENTE")
        j = html.index("  // Secuencia (no un simple booleano)")
        self.assertIn('filename=\'ot_finanzas.js\'', html, "el asistente carga el espejo")
        casos = [
            # (S, líder externo) -> (clase, cobré, queda, valorizado)
            ({"fin_zz_monto": 150000, "fin_zz_monto_original": 150000, "fin_valor_origen": "zz",
              "fin_zz_codigo": "ZZINSTALACION", "fin_zz_envio": {"sku": "ZZENVIO", "monto": 30000},
              "fin_costo_proveedor": "100000", "fin_costo_despacho": "20000"}, True, ["ok", 180000, 60000, None]),
            # 2026-10-07 (revisión): en una OT que se cobra, el chip «Estimado» usado como «Le cobras» es un cobro
            # escrito a mano (pide motivo y viaja 'manual'). Antes la vista previa decía "Falta lo que cobraste"
            # pero la OT se creaba igual y quedaba trabada al cerrar.
            ({"fin_zz_monto": 80000, "fin_zz_monto_original": 80000, "fin_valor_origen": "estimado",
              "fin_costo_proveedor": "50000"}, False, ["ok", 80000, 30000, None]),
            ({"fin_garantia": True, "fin_zz_monto": 200000, "fin_zz_monto_original": 200000,
              "fin_valor_origen": "estimado", "fin_costo_proveedor": "130000", "fin_costo_despacho": "70000"},
             True, ["info", 0, -200000, 200000]),
            ({"fin_zz_monto": 100000, "fin_zz_monto_original": 100000, "fin_valor_origen": "zz",
              "fin_costo_proveedor": "0"}, False, ["ok", 100000, 100000, None]),
        ]
        base = {"fin_garantia": False, "tipo": "instalacion", "fin_zz_monto": None, "fin_zz_monto_original": None,
                "fin_zz_codigo": None, "fin_valor_origen": None, "fin_zz_envio": None, "fin_costo_proveedor": "",
                "fin_costo_despacho": "", "fin_valorizado": "", "fin_cotizacion": None, "fin_costo_estimado": None,
                "fin_zz_motivo_manual": ""}
        js = ["global.window = global;",
              "require(%s);" % json.dumps(os.path.join(RAIZ, "static", "ot_finanzas.js")),
              "const BLOQUE = %s;" % json.dumps(html[i:j]),
              "const CASOS = %s;" % json.dumps([[dict(base, **s), ext] for s, ext, _ in casos]),
              """const out = CASOS.map(function(c){
                   const nodos = {o2mFinMargen: {innerHTML: ''}};
                   const f = new Function('nodos', 'var S = ' + JSON.stringify(c[0]) + ';' +
                     'var document = {getElementById: function(id){ return nodos[id] || null; }};' +
                     'function _o2mLimpiarNumero(v){ return String(v||"").replace(/[^0-9]/g,""); }' +
                     'function esc(s){ return String(s==null?"":s); }' +
                     'function _o2mFinEsManual(){ if (S.fin_zz_monto == null) return false;' +
                     '  return S.fin_zz_monto_original == null || S.fin_zz_monto !== S.fin_zz_monto_original; }' +
                     'function _liderEsExterno(){ return ' + (c[1] ? 'true' : 'false') + '; }' +
                     'function esInterno(){ return false; }' +
                     'function _o2mFinEstadosDom(){}' + BLOQUE +
                     '_pintarFinMargenDom(); var r = _o2mFinVistaPrevia();' +
                     'return [r.clase, r.cobre.total, r.queda.total, r.valorizado.monto, nodos.o2mFinMargen.innerHTML,' +
                     ' _o2mFinMontoServicio(), _o2mFinPideMotivo()];');
                   return f(nodos);
                 });
                 process.stdout.write(JSON.stringify(out));"""]
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write(chr(10).join(js))
            ruta = fh.name
        try:
            r = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
        finally:
            os.unlink(ruta)
        self.assertEqual(r.returncode, 0, r.stderr)
        res = json.loads(r.stdout)
        for (s, ext, esperado), obtenido in zip(casos, res):
            self.assertEqual(obtenido[:4], esperado, s)
        self.assertIn("Me cobraron", res[0][4], "externo: 'Me cobraron' (Daniel 2026-09-27)")
        self.assertIn("Nos costó", res[3][4], "técnico propio: 'Nos costó'")
        self.assertIn("Resultado en caja", res[2][4])
        # Lo que manda crear(): el estimado usado como cobro viaja 'manual' y pide motivo; en garantía no.
        self.assertEqual(res[1][5]["valor_origen"], "manual")
        self.assertTrue(res[1][6], "el estimado usado como cobro pide su motivo")
        self.assertEqual(res[2][5]["valor_origen"], "estimado", "en garantía el estimado es el valorizado")
        self.assertFalse(res[2][6])
        self.assertEqual(res[0][5]["valor_origen"], "zz")
        self.assertFalse(res[0][6])

    def test_el_motivo_se_exige_al_crear(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as fh:
            html = fh.read()
        self.assertIn("if (_o2mFinPideMotivo() && !(S.fin_zz_motivo_manual||'').trim()) return false;", html)
        self.assertIn("if (_o2mFinPideMotivo() && !(S.fin_zz_motivo_manual||'').trim()) return {cls:'falta'", html)


if __name__ == "__main__":
    unittest.main()

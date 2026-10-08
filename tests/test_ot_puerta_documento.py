"""La PUERTA del documento ("documento absoluto"), Daniel 2026-10-07: "Todo con documento tiene que ser absoluto y
solamente pidiendo autorización remota con un argumento podrán solicitarme a mí remotamente que yo autorice
algo… esto tiene que ser inviolable."

Sin BD ni Flask: _ot_puerta_documento se extrae de app.py con ast (mismo patrón que tests/test_ot_finanzas_modelo.py)
y sus lecturas de base (documentos, autorizaciones, contrato real) se reemplazan por stubs.
Correr con:  py -m unittest tests.test_ot_puerta_documento
"""
import ast
import os
import re
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNCS = ("_ot_es_interna", "_ot_cobertura", "_ot_fin_num", "_ot_puerta_documento", "_ot_puerta_documento_eval",
         "_ot_puerta_desde_body", "_ot_puerta_doc_norm", "_ot_puerta_validar_erp", "_ot_puerta_cotizacion_existe",
         "_ot_puerta_autorizacion_por_id", "_ot_puerta_aut_crear_valida", "_ot_puerta_fuera_asistente")
CONSTS = ("_OT_DOCS_NOTA_VENTA", "_OT_DOCS_CIERRE", "_OT_DOCS_COBRO", "_OT_COBRO_CERO_MOTIVOS",
          "_OT_COBRO_CERO_MODALIDAD", "_OT_AUT_TIPOS", "_OT_AUT_ARGUMENTO_MIN", "_OT_PUERTA_NV_CIERRA",
          "_OT_PUERTA_MSG", "_OT_PUERTA_MSG_FACTURA", "_OT_PUERTA_MSG_CERO", "_OT_ESTADOS_NACIMIENTO",
          "_OT_FIN_COBERTURA_TXT", "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ZZ_NO_SERVICIO", "_OT_PUERTA_MSG_FUERA")

_CODIGO = None
_ARBOL = None


def _arbol():
    global _CODIGO, _ARBOL
    if _ARBOL is None:
        _CODIGO, _ARBOL = _codigo_y_arbol()
    return _CODIGO, _ARBOL


def _cargar():
    """Namespace con la puerta y stubs de BD: `docs_bd[vid]`, `auts_bd[vid]`, `contrato_real_bd[vid|cid]`."""
    _, arbol = _arbol()
    amb = {"print": lambda *a, **k: None, "re": re}
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                and nodo.targets[0].id in CONSTS:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in FUNCS + CONSTS if n not in amb]
    assert not faltan, faltan
    amb["docs_bd"], amb["auts_bd"], amb["contrato_real_bd"] = {}, {}, {}
    # _ot_puerta_docs_de devuelve filas ya normalizadas (origen/tido/nudo/es_cobro), como la tabla.
    amb["_ot_puerta_docs_de"] = lambda vid: [dict({"origen": "erp"}, **x) for x in amb["docs_bd"].get(vid, [])]
    amb["_ot_puerta_autorizaciones_de"] = lambda vid: list(amb["auts_bd"].get(vid, []))
    amb["_ot_fin_contrato_real_de"] = lambda vids: {int(v): bool(amb["contrato_real_bd"].get(int(v))) for v in vids}
    amb["_ot_puerta_contrato_real"] = lambda cid, ctid=None: bool(amb["contrato_real_bd"].get(cid) or amb["contrato_real_bd"].get(ctid))
    amb["_ot_resolver_doc_erp"] = lambda t, n, r=None: (None, None, False, None)
    amb["mysql_fetchone"] = lambda *a, **k: None
    amb["mysql_fetchall"] = lambda *a, **k: []
    amb["_ot_aut_es_superadmin"] = lambda *a, **k: False
    amb["_ot_aut_usuario"] = lambda: (1, "Aaron")
    amb["_ot_aut_fila"] = lambda aid: None
    # El RUT del documento contra el del cliente (la comparación real vive en app.py con su dígito verificador).
    amb["_rut_analisis_comparacion"] = lambda a, b: {"match": re.sub(r"[^0-9kK]", "", str(a or "")).upper()
                                                     == re.sub(r"[^0-9kK]", "", str(b or "")).upper()}
    return amb


AMB = None


def P(v, momento="crear", **kw):
    global AMB
    if AMB is None:
        AMB = _cargar()
    base = {"cliente_id": 7, "tipo": "instalacion", "modalidad_cobro": "pagado", "cubierto_por": "cliente",
            "documentos": [], "autorizaciones": [], "contrato_real": False}
    base.update(v)
    # Lo que el SERVIDOR ya validó (tabla puente, validador de finanzas) viaja en `documentos_validados`; lo que escribe
    # una persona, en `documentos` (la bandera `validado` del body NO vale: ver TestNoSeConfiaEnElBody).
    docs = base.get("documentos")
    if docs:
        val = [d for d in docs if d.get("validado")]
        if val:
            base["documentos_validados"] = val
        base["documentos"] = [d for d in docs if not d.get("validado")]
    return AMB["_ot_puerta_documento"](base, momento, **kw)


def PR(v, momento="crear", **kw):
    """La puerta SIN el atajo de P: el body tal cual llega (para probar que no se confía en él)."""
    global AMB
    if AMB is None:
        AMB = _cargar()
    base = {"cliente_id": 7, "tipo": "instalacion", "modalidad_cobro": "pagado", "cubierto_por": "cliente",
            "autorizaciones": [], "contrato_real": False}
    base.update(v)
    return AMB["_ot_puerta_documento"](base, momento, **kw)


# Documentos VALIDADOS (como los deja mant_visita_documentos o el validador). Sin `validado` la puerta los
# confirma contra el ERP, que en esta prueba no existe (stub: nunca responde).
FCV = {"tido": "FCV", "nudo": "11439", "es_cobro": 1, "validado": True}
NVV = {"tido": "NVV", "nudo": "VD00010667", "es_cobro": 1, "validado": True}
COT = {"origen": "cotizacion", "cotizacion": "COT-000045", "validado": True}
APROBADA_CERO = {"id": 5, "tipo": "cobro_cero", "estado": "aprobada", "motivo": "garantia"}
ARG = "El equipo falló a los dos meses de instalado, dentro de la garantía del proveedor."


class TestPuertaCrear(unittest.TestCase):
    def test_con_factura_pasa(self):
        r = P({"documentos": [FCV]})
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "documento")

    def test_con_nota_de_venta_pasa_al_crear_y_queda_falta_factura(self):
        r = P({"documentos": [NVV]})
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "nota_venta")
        self.assertTrue(r.get("nota_venta")); self.assertEqual(r.get("estado_facturacion"), "con_nota_venta")

    def test_sin_nada_no_pasa_y_pide_autorizacion(self):
        r = P({})
        self.assertFalse(r["ok"]); self.assertEqual(r["code"], "DOC_REQUERIDO"); self.assertEqual(r["falta"], "documento")
        self.assertIn("Pedir autorización", r["mensaje"])

    def test_texto_libre_no_cuenta(self):
        # Un número que el ERP no confirma (ni está validado en la tabla) no abre la puerta.
        r = P({"factura_tido": "FCV", "factura_nudo": "999", "documentos": []})
        self.assertFalse(r["ok"])
        r = P({"documentos": [{"tido": "FCV", "nudo": "999"}]})
        self.assertFalse(r["ok"])

    def test_cotizacion_basta_para_crear_pero_no_para_cerrar(self):
        """REQUISITO 3 de Daniel: crear con nota de venta o cotización basta; cerrar exige factura/boleta."""
        r = P({"documentos": [COT]})
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "cotizacion")
        self.assertFalse(P({"documentos": [COT]}, "cerrar")["ok"])

    def test_referencia_de_garantia_no_es_cobro(self):
        self.assertFalse(P({"documentos": [dict(FCV, es_cobro=0)]})["ok"])

    def test_cero_sin_autorizacion_no_pasa(self):
        r = P({"modalidad_cobro": "garantia", "cubierto_por": "garantia", "cobro_cero_motivo": "garantia",
               "cobro_cero_argumento": ARG})
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "autorizacion_cobro_cero")

    def test_cero_autorizado_pasa(self):
        r = P({"modalidad_cobro": "garantia", "cubierto_por": "garantia", "cobro_cero_motivo": "garantia",
               "cobro_cero_autorizacion_id": 5, "autorizaciones": [APROBADA_CERO]})
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "cobro_cero"); self.assertEqual(r["autorizacion_id"], 5)

    def test_regalia_y_arriendo_son_motivos_de_cero(self):
        for m in ("regalia", "arriendo_leasing"):
            r = P({"modalidad_cobro": "sin_costo", "cobro_cero_motivo": m,
                   "autorizaciones": [dict(APROBADA_CERO, motivo=m)]})
            self.assertTrue(r["ok"], m); self.assertEqual(r["motivo"], m)
        self.assertEqual(AMB["_OT_COBRO_CERO_MODALIDAD"]["regalia"], "sin_costo")
        self.assertEqual(AMB["_OT_COBRO_CERO_MODALIDAD"]["garantia"], "garantia")
        self.assertEqual(set(AMB["_OT_COBRO_CERO_MOTIVOS"]), {"garantia", "regalia", "arriendo_leasing"})

    def test_superadmin_declara_el_cero_con_argumento(self):
        r = P({"modalidad_cobro": "sin_costo", "cobro_cero_motivo": "regalia", "cobro_cero_argumento": ARG},
              superadmin_declara=True)
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "cobro_cero_superadmin")

    def test_superadmin_sin_argumento_largo_no_pasa(self):
        r = P({"modalidad_cobro": "sin_costo", "cobro_cero_motivo": "regalia", "cobro_cero_argumento": "corto"},
              superadmin_declara=True)
        self.assertFalse(r["ok"])
        self.assertGreaterEqual(AMB["_OT_AUT_ARGUMENTO_MIN"], 30)

    def test_interna_sin_cliente_pasa_siempre(self):
        r = P({"cliente_id": None, "modalidad_cobro": "interno"})
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "interna_sin_cliente")

    def test_modalidad_interno_con_cliente_no_exime(self):
        r = P({"cliente_id": 7, "modalidad_cobro": "interno", "tipo": "revision_interna"})
        self.assertFalse(r["ok"])

    def test_preventiva_contrato_real_pasa_contenedor_no(self):
        self.assertTrue(P({"tipo": "preventiva", "contrato_real": True})["ok"])
        self.assertFalse(P({"tipo": "preventiva", "contrato_real": False})["ok"])

    def test_preventiva_de_contrato_con_cobro_facturado(self):
        # Si se facturó, pasa por el documento; el contrato real igual la deja pasar: nunca se traba.
        r = P({"tipo": "preventiva", "contrato_real": True, "documentos": [FCV], "zz_monto": 50000, "valor_origen": "zz"})
        self.assertTrue(r["ok"])

    def test_correctiva_con_contrato_real_no_exime(self):
        self.assertFalse(P({"tipo": "correctiva", "contrato_real": True})["ok"])

    def test_autorizacion_crear_sin_documento_aprobada(self):
        a = {"id": 9, "tipo": "crear_sin_documento", "estado": "aprobada", "solicitado_por_user_id": 1, "cliente_id": 7}
        r = P({}, autorizacion=a)
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "autorizacion_crear"); self.assertEqual(r["autorizacion_id"], 9)
        self.assertFalse(P({}, autorizacion=dict(a, estado="pendiente"))["ok"])
        self.assertFalse(P({}, autorizacion=dict(a, tipo="cerrar_sin_documento"))["ok"])


class TestNoSeConfiaEnElBody(unittest.TestCase):
    """Revisión adversarial 2026-10-08: lo que dice el navegador no abre la puerta."""

    def setUp(self):
        P({})   # carga AMB
        self._res = AMB["_ot_resolver_doc_erp"]

    def tearDown(self):
        AMB["_ot_resolver_doc_erp"] = self._res

    def test_validado_del_body_se_ignora(self):
        r = PR({"documentos": [{"tipo": "FCV", "numero": "999999", "validado": True}]})
        self.assertFalse(r["ok"])
        r = PR({"documentos": [{"origen": "cotizacion", "cotizacion": "COT-9", "validado": True}]})
        self.assertFalse(r["ok"])

    def test_es_cobro_y_rut_del_body_se_ignoran(self):
        AMB["_ot_resolver_doc_erp"] = lambda t, n, r=None: ("FCV", {"cliente_rut": "11.111.111-1"}, True, None)
        r = PR({"documentos": [{"tipo": "FCV", "numero": "55", "es_cobro": False, "rut": "99.999.999-9"}],
                "cliente_rut": "11.111.111-1"})
        self.assertTrue(r["ok"]); self.assertTrue(r["documentos_ok"][0]["es_cobro"])
        self.assertEqual(r["documentos_ok"][0]["rut"], "11.111.111-1")

    def test_el_documento_se_confirma_contra_el_erp(self):
        AMB["_ot_resolver_doc_erp"] = lambda t, n, r=None: ("FCV", {"cliente_rut": "11.111.111-1"}, True, None)
        self.assertTrue(PR({"documentos": [{"tipo": "FCV", "numero": "55"}], "cliente_rut": "11.111.111-1"})["ok"])

    def test_factura_de_otro_rut_no_sirve(self):
        AMB["_ot_resolver_doc_erp"] = lambda t, n, r=None: ("FCV", {"cliente_rut": "22.222.222-2"}, True, None)
        r = PR({"documentos": [{"tipo": "FCV", "numero": "55"}], "cliente_rut": "11.111.111-1"})
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "documento")

    def test_los_validados_por_el_servidor_si_cuentan_y_conservan_es_cobro(self):
        r = PR({"documentos_validados": [{"origen": "erp", "tipo": "FCV", "numero": "55", "validado": True}]})
        self.assertTrue(r["ok"])
        r = PR({"documentos_validados": [{"origen": "erp", "tipo": "FCV", "numero": "55", "es_cobro": False}]})
        self.assertFalse(r["ok"])   # referencia de garantía: no cobra

    def test_desde_body_nace_completada_se_revisa_como_cerrar(self):
        AMB["_ot_resolver_doc_erp"] = lambda t, n, r=None: ("NVV", {"cliente_rut": ""}, True, None)
        d = {"documentos": [{"tipo": "NVV", "numero": "VD00010667"}]}
        self.assertTrue(AMB["_ot_puerta_desde_body"](d, 7, "correctiva")["ok"])                       # crear: basta la nota de venta
        r = AMB["_ot_puerta_desde_body"](d, 7, "correctiva", momento="cerrar")                          # nace completada: no
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "factura")

    def test_cotizacion_de_otro_cliente_o_rechazada_no_sirve(self):
        previa = AMB["mysql_fetchone"]
        try:
            AMB["mysql_fetchone"] = lambda *a, **k: {"id": 1, "rut": "22.222.222-2", "estado": "draft"}
            d = {"documentos": [{"origen": "cotizacion", "cotizacion": "COT-000001"}], "cliente_rut": "11.111.111-1"}
            self.assertFalse(PR(d)["ok"])
            AMB["mysql_fetchone"] = lambda *a, **k: {"id": 1, "rut": "11.111.111-1", "estado": "draft"}
            self.assertTrue(PR(d)["ok"])
            AMB["mysql_fetchone"] = lambda *a, **k: {"id": 1, "rut": "11.111.111-1", "estado": "rejected"}
            self.assertFalse(PR(d)["ok"])
        finally:
            AMB["mysql_fetchone"] = previa

    def test_fuera_del_asistente_el_mensaje_dice_por_donde_seguir(self):
        r = AMB["_ot_puerta_desde_body"]({}, 7, "correctiva")
        self.assertFalse(r["ok"]); self.assertTrue(r.get("fuera_asistente"))
        self.assertIn("Nueva OT", r["mensaje"]); self.assertIn("Pedir autorización", r["mensaje"])


class TestAutorizacionesAjenas(unittest.TestCase):
    """Una autorización aprobada vale para SU cliente, SU OT y quien la pidió."""

    def test_crear_sin_documento_de_otro_cliente_no_sirve(self):
        a = {"id": 9, "tipo": "crear_sin_documento", "estado": "aprobada", "solicitado_por_user_id": 1, "cliente_id": 99}
        self.assertFalse(P({"cliente_id": 7}, autorizacion=a)["ok"])

    def test_crear_sin_documento_de_otra_persona_no_sirve(self):
        a = {"id": 9, "tipo": "crear_sin_documento", "estado": "aprobada", "solicitado_por_user_id": 2, "cliente_id": 7}
        self.assertFalse(P({}, autorizacion=a)["ok"])

    def test_superadmin_que_aprueba_y_crea_si_pasa(self):
        P({})
        previa = AMB["_ot_aut_es_superadmin"]
        AMB["_ot_aut_es_superadmin"] = lambda *a, **k: True
        try:
            a = {"id": 9, "tipo": "crear_sin_documento", "estado": "aprobada", "solicitado_por_user_id": 2, "cliente_id": 7}
            self.assertTrue(P({}, autorizacion=a)["ok"])
        finally:
            AMB["_ot_aut_es_superadmin"] = previa

    def test_cobro_cero_aprobado_no_sirve_para_crear_otra_ot(self):
        a = {"id": 5, "tipo": "cobro_cero", "estado": "aprobada", "motivo": "garantia", "visita_id": 123}
        r = P({"modalidad_cobro": "garantia", "cobro_cero_motivo": "garantia", "cobro_cero_argumento": ARG}, autorizacion=a)
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "autorizacion_cobro_cero")

    def test_cobro_cero_de_otra_ot_no_sirve_al_cerrar(self):
        a = {"id": 5, "tipo": "cobro_cero", "estado": "aprobada", "motivo": "garantia", "visita_id": 123}
        v = {"id": 200, "modalidad_cobro": "garantia", "cobro_cero_motivo": "garantia"}
        self.assertFalse(P(v, "cerrar", autorizacion=a)["ok"])
        self.assertTrue(P(dict(v, id=123), "cerrar", autorizacion=a)["ok"])

    def test_cerrar_sin_documento_de_otra_ot_no_sirve(self):
        a = {"id": 3, "tipo": "cerrar_sin_documento", "estado": "aprobada", "visita_id": 123}
        self.assertFalse(P({"id": 200}, "cerrar", autorizacion=a)["ok"])
        self.assertTrue(P({"id": 123}, "cerrar", autorizacion=a)["ok"])


class TestPuertaCerrar(unittest.TestCase):
    def test_factura_cierra(self):
        self.assertTrue(P({"documentos": [FCV]}, "cerrar")["ok"])

    def test_nota_de_venta_ya_no_cierra(self):
        """REQUISITO 3 de Daniel (anula la regla del 19-08 y las excepciones de arriendo/leasing y superadmin)."""
        r = P({"documentos": [NVV], "cliente_tipo": "arriendo"}, "cerrar")
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "factura")
        self.assertFalse(AMB["_OT_PUERTA_NV_CIERRA"])

    def test_cero_autorizado_cierra(self):
        r = P({"modalidad_cobro": "garantia", "cobro_cero_motivo": "garantia", "cobro_cero_autorizacion_id": 5,
               "autorizaciones": [APROBADA_CERO]}, "cerrar")
        self.assertTrue(r["ok"])

    def test_garantia_antigua_sin_autorizacion_no_cierra(self):
        r = P({"modalidad_cobro": "garantia", "cubierto_por": "garantia"}, "cerrar")
        self.assertFalse(r["ok"]); self.assertEqual(r["falta"], "autorizacion_cobro_cero")

    def test_autorizacion_cerrar_sin_documento(self):
        r = P({"autorizaciones": [{"id": 3, "tipo": "cerrar_sin_documento", "estado": "aprobada"}]}, "cerrar")
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "autorizacion_cerrar")
        r2 = P({"autorizaciones": [{"id": 3, "tipo": "cerrar_sin_documento", "estado": "pendiente"}]}, "cerrar")
        self.assertFalse(r2["ok"])

    def test_autorizacion_crear_no_sirve_para_cerrar(self):
        r = P({"autorizaciones": [{"id": 9, "tipo": "crear_sin_documento", "estado": "aprobada"}]}, "cerrar")
        self.assertFalse(r["ok"])

    def test_lee_de_bd_cuando_viene_id(self):
        AMB["docs_bd"][40] = [FCV]
        r = P({"id": 40, "documentos": None, "autorizaciones": None}, "cerrar")
        self.assertTrue(r["ok"]); self.assertEqual(r["via"], "documento")
        AMB["docs_bd"][41] = []
        AMB["auts_bd"][41] = [{"id": 1, "tipo": "cerrar_sin_documento", "estado": "aprobada"}]
        self.assertTrue(P({"id": 41, "documentos": None, "autorizaciones": None}, "cerrar")["ok"])


# ── Prueba estática: TODOS los caminos que crean una OT de cliente pasan por la puerta ────────────────────────
PUERTA_NOMBRES = ("_ot_puerta_documento", "_ot_puerta_desde_body", "_ot_puerta_contrato_real",
                  "_ot_puerta_documento_eval", "_ot_contrato_es_real")
# Funciones que insertan en mant_visitas sin llamar a la puerta ELLAS MISMAS: el espejo del levantamiento solo lo
# llama _mant_lev_crear_ot_core, que ya pasó la puerta (ver test_nucleo_de_levantamiento_pasa_la_puerta_antes_del_espejo)
# y le deja la constancia en fin_campos["_puerta"].
EXCEPCIONES_INSERT = {"_ot_crear_visita_espejo"}


def _funciones_con_insert(codigo, arbol):
    out = []
    lineas = codigo.splitlines()
    for nodo in arbol.body:
        if not isinstance(nodo, ast.FunctionDef):
            continue
        src = "\n".join(lineas[nodo.lineno - 1:nodo.end_lineno])
        if re.search(r"INSERT INTO mant_visitas\b", src):
            out.append((nodo.name, src))
    return out


class TestTodosLosCaminos(unittest.TestCase):
    def test_cada_insert_de_mant_visitas_esta_detras_de_la_puerta(self):
        codigo, arbol = _arbol()
        funciones = _funciones_con_insert(codigo, arbol)
        self.assertGreaterEqual(len(funciones), 12, [f for f, _ in funciones])
        sin_puerta = [f for f, src in funciones
                      if f not in EXCEPCIONES_INSERT and not any(n + "(" in src for n in PUERTA_NOMBRES)]
        self.assertEqual(sin_puerta, [], f"INSERT INTO mant_visitas sin pasar por la puerta: {sin_puerta}")

    def test_caminos_del_mapa_siguen_existiendo(self):
        codigo, arbol = _arbol()
        nombres = {f for f, _ in _funciones_con_insert(codigo, arbol)}
        for f in ("_ot2_crear_core", "_mant_visita_crear_core", "mant_visita_multi", "mant_maquina_solicitar_cambio",
                  "mant_visita_historica", "mant_visita_retroactiva", "mant_contrato_auto_calendar",
                  "mant_generar_calendario", "mant_planificador_generar_ots", "mant_intel_accion",
                  "_ot_crear_visita_espejo", "_mantenciones_cron_run_once"):
            self.assertIn(f, nombres, f)

    def test_tickets_generar_ot_usa_el_nucleo_compartido(self):
        with open(os.path.join(RAIZ, "tickets_module.py"), encoding="utf-8") as fh:
            tk = fh.read()
        self.assertNotIn("INSERT INTO mant_visitas", tk)
        self.assertIn("_mant_lev_crear_ot_core(cliente_id, lev_payload", tk)

    def test_nucleo_de_levantamiento_pasa_la_puerta_antes_del_espejo(self):
        codigo, arbol = _arbol()
        src = dict(_funciones_con_insert(codigo, arbol))
        lev = next(("\n".join(codigo.splitlines()[n.lineno - 1:n.end_lineno]) for n in arbol.body
                    if isinstance(n, ast.FunctionDef) and n.name == "_mant_lev_crear_ot_core"), "")
        self.assertIn("_ot_puerta_documento(", lev)
        self.assertIn('"crear"', lev)
        self.assertIn("_ot_puerta_aplicar(", src["_ot_crear_visita_espejo"], "el espejo deja la constancia")


def _src(nombre):
    codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return "\n".join(codigo.splitlines()[n.lineno - 1:n.end_lineno])
    raise AssertionError(f"no existe {nombre}")


class TestCandadosDeRutas(unittest.TestCase):
    def test_aprobar_y_rechazar_exigen_superadmin(self):
        codigo, arbol = _arbol()
        rutas = {}
        for n in arbol.body:
            if isinstance(n, ast.FunctionDef):
                for d in n.decorator_list:
                    s = ast.get_source_segment(codigo, d) or ""
                    if "/ot/api/autorizaciones/<int:aid>/aprobar" in s:
                        rutas["aprobar"] = n.name
                    if "/ot/api/autorizaciones/<int:aid>/rechazar" in s:
                        rutas["rechazar"] = n.name
        self.assertEqual(set(rutas), {"aprobar", "rechazar"})
        for k, fn in rutas.items():
            src = _src(fn)
            self.assertTrue("_ot_aut_es_superadmin(" in src or "_ot_aut_resolver_guard(" in src, k)
        guard = _src("_ot_aut_resolver_guard")
        self.assertIn("_ot_aut_es_superadmin(", guard); self.assertIn("SOLO_SUPERADMIN", guard)
        self.assertIn("comentario", _src(rutas["rechazar"]))

    def test_interruptor_del_cierre_no_se_salta_con_mayusculas(self):
        src = _src("mant_reglas_guardar")
        self.assertRegex(src, r'str\(clave\)\.strip\(\)\.lower\(\)\s*==\s*"ot_factura_gate_activo"')

    def test_put_no_cambia_el_tipo_a_preventiva_ni_el_documento_ligado(self):
        src = _src("mant_visita_update")
        self.assertIn("TIPO_PROTEGIDO", src)
        self.assertIn("DOCUMENTO_PROTEGIDO", src)
        self.assertIn("_ot_factura_escritura_protegida(", src)
        self.assertIn("_ot_factura_escritura_protegida(", _src("ot2_api_finanzas"))

    def test_retractar_el_cero_limpia_la_constancia(self):
        for fn in ("ot2_api_finanzas", "mant_ot_declarar_cobertura"):
            src = _src(fn)
            self.assertIn("cobro_cero_motivo", src, fn)
            self.assertRegex(src, r"cobro_cero_autorizacion_id(=NULL|\", None)", fn)
        self.assertIn("cobro_cero_motivo", _src("mant_ot_aprobar_cierre").split("FROM mant_visitas v")[0])

    def test_regularizar_no_vuelve_al_erp_por_cada_ot(self):
        self.assertIn("documentos_validados", _src("ot_api_regularizar"))

    def test_cerrar_ruta_vieja_pasa_la_puerta_y_el_centro(self):
        src = _src("mant_visita_cerrar")
        self.assertIn('_ot_puerta_documento(', src); self.assertIn("SIN_CENTRO_COSTO", src)

    def test_levantamiento_exige_centro_y_no_falla_abierto(self):
        src = _src("mant_lev_cerrar")
        self.assertIn("SIN_CENTRO_COSTO", src)
        self.assertIn("REVISION_NO_DISPONIBLE", src)

    def test_historica_y_retroactiva_nacen_completadas_y_se_revisan_al_cerrar(self):
        for fn in ("mant_visita_historica", "mant_visita_retroactiva"):
            self.assertIn('momento="cerrar"', _src(fn), fn)

    def test_ot_creada_por_autorizacion_queda_a_nombre_de_quien_la_pidio(self):
        self.assertIn("solicitado_por_nombre", _src("_ot2_crear_core"))

    def test_nucleo_espeja_los_documentos_del_body_que_confirmo_la_puerta(self):
        self.assertIn("docs_espejados", _src("_ot2_crear_core"))
        self.assertIn("docs_espejados", _src("_ot_puerta_aplicar"))

    def test_interruptor_del_cierre_solo_superadmin(self):
        src = _src("mant_reglas_guardar")
        self.assertIn("ot_factura_gate_activo", src)
        self.assertIn("_ot_aut_es_superadmin(", src)
        self.assertIn("SOLO_SUPERADMIN", src)

    def test_cierre_lee_el_interruptor_fresco(self):
        src = _src("mant_ot_aprobar_cierre")
        self.assertIn('_regla_fresca("ot_factura_gate_activo"', src)
        self.assertIn('_ot_puerta_documento(', src)
        self.assertIn('"cerrar"', src)

    def test_put_no_deja_poner_completada_ni_cerrada(self):
        src = _src("mant_visita_update")
        m = re.search(r"_ESTADOS_PROTEGIDOS_PUT\s*=\s*\{([^}]*)\}", src)
        self.assertIsNotNone(m)
        protegidos = set(re.findall(r'"([a-z_]+)"', m.group(1)))
        self.assertTrue({"completada", "cerrada", "pendiente_aprobacion", "firmada_tecnico"} <= protegidos, protegidos)

    def test_nucleo_clasico_estado_inicial_lista_blanca(self):
        src = _src("_mant_visita_crear_core")
        self.assertIn("_OT_ESTADOS_NACIMIENTO", src)
        self.assertNotIn('d.get("estado","programada")', src)
        for e in AMB_estados():
            self.assertNotIn(e, ("completada", "cerrada", "cancelada", "anulada", "pendiente_aprobacion"))

    def test_garantia_despues_de_crear_pasa_por_autorizacion(self):
        for fn in ("mant_visita_update", "mant_ot_declarar_cobertura", "ot2_api_finanzas"):
            self.assertIn("_ot_cobro_cero_desde_peticion(", _src(fn), fn)

    def test_cierre_del_levantamiento_pasa_la_puerta(self):
        src = _src("mant_lev_cerrar")
        self.assertIn("_ot_puerta_documento(", src)
        self.assertIn('"cerrar"', src)


def AMB_estados():
    global AMB
    if AMB is None:
        AMB = _cargar()
    return AMB["_OT_ESTADOS_NACIMIENTO"]


if __name__ == "__main__":
    unittest.main()

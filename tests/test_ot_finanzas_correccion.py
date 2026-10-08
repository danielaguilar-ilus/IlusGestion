"""Corregir finanzas de una OT (solo superadmin, con registro) — Daniel, 2026-10-07.

Sin BD ni Flask: se extraen las funciones de app.py con ast y se ejecutan con una BD simulada.
2026-10-07 (modelo único): la ganancia y los motivos de "dudosas" salen de la cuenta única _ot_finanzas, así
que el ámbito carga también esa función y sus dependencias (antes cada una tenía su propia fórmula).
Correr con:  py -m unittest tests.test_ot_finanzas_correccion
"""
import json
import os
import re
import types
import unittest

import ast

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FILA = {"id": 211, "numero_ot": "OT-2026-00211", "estado": "cerrada", "tipo": "instalacion", "cliente_id": 5,
        "zz_monto": 239200, "zz_envio_monto": 0, "costo_proveedor": None, "costo_despacho": None,
        "centro_costo": None, "modalidad_cobro": "pago", "cubierto_por": None}


class _Cur:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))


class _Conn:
    def __init__(self):
        self.log, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class _Req:
    def __init__(self, cuerpo):
        self._c = cuerpo

    def get_json(self, silent=True):
        return self._c


def _cargar(funciones, constantes, extra=None):
    """Extrae funciones (SIN sus decoradores de Flask) y constantes de app.py y las ejecuta juntas."""
    _, arbol = _codigo_y_arbol()
    amb = dict(extra or {})
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name)                 and nodo.targets[0].id in constantes:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in funciones:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in tuple(funciones) + tuple(constantes) if n not in amb]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return amb


def _ambito(superadmin=True, cuerpo=None, fila=FILA):
    conn = _Conn()
    mant_log = []
    amb = _cargar(
        ["ot2_api_finanzas_corregir", "_ot_fin_ganancia", "_ot_fin_es_superadmin", "_ot_fin_dudosas_motivos",
         "_ot2_err", "_ot_fin_rep_de",
         # la cuenta única y lo que usa
         "_ot_finanzas", "_ot_cobertura", "_ot_es_interna", "_ot_fin_num", "_ot_fin_clp"],
        ["_OT_FIN_CORR_CAMPOS", "_OT_FIN_CORR_ROT", "_OT_FIN_CORR_MOTIVO_MIN", "_OT_FIN_SQL_CONTRATO_REAL",
         "_OT_FIN_SELECT", "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO",
         "_OT_FIN_COBERTURA_TXT", "_OT_FIN_UMBRAL_BAJO", "_OT2_LINEA_ZZ"],
        extra={
            "json": json, "print": lambda *a, **k: None,
            "g": types.SimpleNamespace(permissions={"superadmin": superadmin}),
            "request": _Req(cuerpo or {}), "jsonify": lambda d: d,
            "mysql_fetchone": lambda sql, p=None: dict(fila) if fila else None,
            "get_mysql": lambda: conn, "current_username": lambda: "daniel",
            "_mant_log": lambda *a, **k: mant_log.append(a),
            "_OT2_CENTROS_COSTO": [("sstt", "SSTT"), ("logistica", "Logística"), ("comercial", "Comercial")],
            "_ot_repuestos_desglose": lambda vids: {},
        })
    # las vistas llevan decoradores de Flask: se llama a la función sin ellos
    amb["_conn"], amb["_mant_log_reg"] = conn, mant_log
    return amb


def _llamar(amb):
    f = amb["ot2_api_finanzas_corregir"]
    return f(211)


class TestCorreccion(unittest.TestCase):
    def test_solo_superadmin(self):
        amb = _ambito(superadmin=False, cuerpo={"motivo": "motivo largo suficiente", "costo_proveedor": 100})
        cuerpo, http = _llamar(amb)
        self.assertEqual(http, 403)
        self.assertEqual(amb["_conn"].log, [], "no escribe nada")

    def test_exige_motivo(self):
        cuerpo, http = _llamar(_ambito(cuerpo={"costo_proveedor": 100, "motivo": "corto"}))
        self.assertEqual(http, 400)
        self.assertEqual(cuerpo["codigo"], "MOTIVO_CORTO")

    def test_caso_ot_211_registra_antes_y_despues_y_recalcula_la_ganancia(self):
        amb = _ambito(cuerpo={"costo_proveedor": 150000, "motivo": "El costo del técnico no se declaró al cerrar"})
        cuerpo = _llamar(amb)
        self.assertTrue(cuerpo["ok"])
        self.assertEqual(cuerpo["ganancia_antes"], 239200)
        self.assertEqual(cuerpo["ganancia_despues"], 89200)
        sqls = [s for s, _ in amb["_conn"].log]
        self.assertTrue(sqls[0].startswith("UPDATE mant_visitas SET costo_proveedor=%s WHERE id=%s"))
        ins = amb["_conn"].log[1]
        self.assertIn("INSERT INTO mant_ot_finanzas_correcciones", ins[0])
        self.assertEqual(json.loads(ins[1][4]), {"costo_proveedor": None})
        self.assertEqual(json.loads(ins[1][5]), {"costo_proveedor": 150000})
        self.assertEqual(amb["_conn"].commits, 1)
        self.assertEqual(len(amb["_mant_log_reg"]), 1, "queda en la bitácora de la OT")

    def test_jamas_toca_firmas_estado_cliente_ni_documentos(self):
        amb = _ambito(cuerpo={"costo_proveedor": 1, "zz_monto": 2, "centro_costo": "sstt",
                              "firma_cliente_at": "x", "estado": "abierta", "cliente_id": 9,
                              "factura_nudo": "1", "motivo": "corrección de prueba suficientemente larga"})
        _llamar(amb)
        update = amb["_conn"].log[0][0]
        for prohibido in ("firma", "estado", "cliente_id", "factura", "fecha"):
            self.assertNotIn(prohibido, update)

    def test_sin_cambios_no_escribe(self):
        cuerpo, http = _llamar(_ambito(cuerpo={"zz_monto": 239200, "motivo": "no cambia nada realmente aquí"}))
        self.assertEqual((http, cuerpo["codigo"]), (400, "SIN_CAMBIOS"))

    def test_rechaza_montos_invalidos(self):
        for malo in ("abc", -5):
            cuerpo, http = _llamar(_ambito(cuerpo={"costo_proveedor": malo, "motivo": "motivo largo suficiente"}))
            self.assertEqual(http, 400, malo)

    def test_vaciar_un_casillero_quita_ese_monto(self):
        # Daniel, OT-201: el $1 de ZZRETIRO no es un cobro y no se podía borrar
        amb = _ambito(cuerpo={"zz_monto": "", "motivo": "El $1 es un retiro en bodega, no un cobro del servicio"})
        cuerpo = _llamar(amb)
        self.assertTrue(cuerpo["ok"])
        self.assertEqual(cuerpo["cambios"]["zz_monto"], {"antes": 239200, "despues": None})
        self.assertTrue(amb["_conn"].log[0][0].startswith("UPDATE mant_visitas SET zz_monto=%s WHERE id=%s"))
        self.assertEqual(amb["_conn"].log[0][1], (None, 211))

    def test_centro_de_costo_inexistente(self):
        cuerpo, http = _llamar(_ambito(cuerpo={"centro_costo": "otro", "motivo": "motivo largo suficiente"}))
        self.assertEqual(cuerpo["codigo"], "CENTRO_INVALIDO")

    def test_en_garantia_la_ganancia_no_cuenta_cobro(self):
        fila = dict(FILA, modalidad_cobro="garantia")
        amb = _ambito(cuerpo={"costo_proveedor": 1000, "motivo": "motivo largo suficiente"}, fila=fila)
        cuerpo = _llamar(amb)
        self.assertEqual(cuerpo["ganancia_antes"], 0)
        self.assertEqual(cuerpo["ganancia_despues"], -1000)

    # ── 2026-10-07: modelo único ──────────────────────────────────────────────
    def test_la_ganancia_es_la_misma_cuenta_de_la_tarjeta(self):
        # OT-201 en garantía: el $1 de ZZRETIRO y el «Precio al cliente» no son cobro; queda lo que nos costó.
        fila = dict(FILA, modalidad_cobro="garantia", costo=200000, zz_monto=1, zz_codigo="ZZRETIRO",
                    valor_origen="zz", costo_proveedor=130000, costo_despacho=70000)
        amb = _ambito(fila=fila)
        self.assertEqual(amb["_ot_fin_ganancia"](fila), -200000)
        # con repuestos instalados, también suman a lo que me cobraron
        rep = {"costo": 5000, "por_origen": {"bodega": 5000, "compra": 0, "manual": 0}, "n_sin_costo": 0}
        self.assertEqual(amb["_ot_fin_ganancia"](fila, rep), -205000)

    def test_corregir_el_cobro_lo_deja_escrito_a_mano_y_fuera_de_zzretiro(self):
        fila = dict(FILA, zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz", costo_proveedor=130000,
                    costo_despacho=70000)
        amb = _ambito(cuerpo={"zz_monto": 250000, "motivo": "El cobro real fue la instalación, no el retiro"},
                      fila=fila)
        cuerpo = _llamar(amb)
        self.assertTrue(cuerpo["ok"])
        self.assertEqual(cuerpo["cambios"]["valor_origen"], {"antes": "zz", "despues": "manual"})
        self.assertEqual(cuerpo["cambios"]["zz_codigo"], {"antes": "ZZRETIRO", "despues": "ZZINSTALACION"})
        self.assertEqual(cuerpo["ganancia_despues"], 50000, "250.000 cobrados − 200.000 que me cobraron")
        sql, params = amb["_conn"].log[0]
        self.assertIn("valor_origen=%s", sql)
        self.assertIn("zz_codigo=%s", sql)

    def test_el_valorizado_se_corrige_y_queda_a_mano(self):
        amb = _ambito(cuerpo={"valorizado_clp": 180000, "motivo": "Tarifa real según la cotización 123"})
        cuerpo = _llamar(amb)
        self.assertEqual(cuerpo["cambios"]["valorizado_clp"], {"antes": None, "despues": 180000})
        self.assertEqual(cuerpo["cambios"]["valorizado_fuente"], {"antes": None, "despues": "a_mano"})
        self.assertEqual(cuerpo["ganancia_antes"], cuerpo["ganancia_despues"], "el valorizado no es cobro")


class TestInformeDudosas(unittest.TestCase):
    def setUp(self):
        self.motivos = _ambito()["_ot_fin_dudosas_motivos"]

    def test_la_ot_211_sale_marcada(self):
        m = self.motivos(dict(FILA, proveedor_tipo="externo"))
        self.assertIn("Sin centro de costo", m)
        self.assertIn("Técnico externo sin costo declarado: la ganancia sale inflada", m)

    def test_una_ot_sana_no_sale(self):
        sana = dict(FILA, centro_costo="sstt", costo_proveedor=100000, costo_despacho=0, proveedor_tipo="externo")
        self.assertEqual(self.motivos(sana), [])

    def test_perdida(self):
        perdida = dict(FILA, centro_costo="sstt", costo_proveedor=300000, costo_despacho=0, proveedor_tipo="externo")
        self.assertIn("Pérdida: lo que cuesta supera lo cobrado", self.motivos(perdida))

    def test_garantia_sin_cobro_no_es_duda(self):
        gar = dict(FILA, centro_costo="sstt", modalidad_cobro="garantia", zz_monto=0, costo_proveedor=50000)
        m = self.motivos(gar)
        self.assertNotIn("Sin cobro declarado (y no es garantía)", m)
        self.assertNotIn("Sin cobro declarado (y la OT se cobra)", m)
        # lo que nos costó una garantía no es una "pérdida"
        self.assertNotIn("Pérdida: lo que cuesta supera lo cobrado", m)

    def test_ot_que_se_cobra_sin_cobro(self):
        m = self.motivos(dict(FILA, zz_monto=None, zz_envio_monto=None, centro_costo="sstt", costo_proveedor=1000))
        self.assertIn("Sin cobro declarado (y la OT se cobra)", m)
        # un despacho declarado en $0 sí es un dato: no es "sin cobro", es un cobro de $0
        m = self.motivos(dict(FILA, zz_monto=None, centro_costo="sstt", costo_proveedor=1000))
        self.assertIn("Cobro declarado en $0 (y la OT se cobra)", m)
        self.assertIn("Pérdida: lo que cuesta supera lo cobrado", m)

    def test_motivos_nuevos_del_modelo_unico(self):
        ot201 = dict(FILA, centro_costo="sstt", modalidad_cobro="garantia", costo=200000, zz_monto=1,
                     zz_codigo="ZZRETIRO", valor_origen="zz", costo_proveedor=130000, costo_despacho=70000)
        self.assertIn("Línea ZZ que no es de servicio", self.motivos(ot201))
        distinto = dict(FILA, centro_costo="sstt", costo=300000, costo_proveedor=100000, costo_despacho=0)
        self.assertIn("Precio al cliente distinto de lo cobrado", self.motivos(distinto))
        antiguo = dict(FILA, centro_costo="sstt", zz_monto=None, costo=300000, costo_proveedor=100000,
                       costo_despacho=0, valor_origen="cotizacion")
        self.assertIn("Sin separar servicio y despacho", self.motivos(antiguo))
        # 2026-10-08 (revision): con la linea vaciada, 'doc_total' ya no respalda el precio: es un precio anotado.
        vaciada = dict(FILA, centro_costo="sstt", zz_monto=None, costo=300000, costo_proveedor=100000,
                       costo_despacho=0, valor_origen="doc_total")
        self.assertIn("Precio anotado sin documento de cobro (no cuenta como cobro)", self.motivos(vaciada))
        # 2026-10-08: un precio anotado SIN respaldo (sin documento ni cobro declarado) no es un cobro.
        anotado = dict(FILA, centro_costo="sstt", zz_monto=None, costo=300000, costo_proveedor=100000,
                       costo_despacho=0, valor_origen=None)
        m = self.motivos(anotado)
        self.assertIn("Precio anotado sin documento de cobro (no cuenta como cobro)", m)
        self.assertIn("Cobro declarado en $0 (y la OT se cobra)", m)   # FILA trae el despacho en $0
        self.assertNotIn("Sin separar servicio y despacho", m)


class TestRutasYTablas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            cls.app = fh.read().replace(chr(13) + chr(10), chr(10))

    def test_la_tabla_de_registro_se_crea_siempre_al_arrancar(self):
        self.assertIn("_ensure_ot_finanzas_correcciones()", self.app.split("# Multidocumento de la OT")[0][-900:])

    def test_el_registro_nunca_se_actualiza_ni_se_borra(self):
        self.assertNotIn("UPDATE mant_ot_finanzas_correcciones", self.app)
        self.assertNotIn("DELETE FROM mant_ot_finanzas_correcciones", self.app)

    def test_las_tres_rutas_exigen_superadmin(self):
        for nombre in ("ot2_api_finanzas_corregir", "ot2_api_finanzas_correcciones", "ot2_finanzas_dudosas"):
            i = self.app.index(f"def {nombre}(")
            self.assertIn("_ot_fin_es_superadmin()", self.app[i:i + 700], nombre)

    def test_la_ui_solo_se_dibuja_para_superadmin(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            html = fh.read()
        # 2026-10-08 (un solo bloque compacto): «Corregir finanzas» y «OT con finanzas dudosas» pasaron de la tarjeta vieja al
        # ENCABEZADO del motor (templates/ot2/_fin_motor.html, junto a «Actualizar»), con el MISMO permiso: solo se dibujan para
        # superadmin y solo en la ficha (nunca en el modal de cierre). Antes era un <div class="otd-fincorr-bar"> en detalle.html.
        with open(os.path.join(RAIZ, "templates", "ot2", "_fin_motor.html"), encoding="utf-8") as fh:
            motor = fh.read().replace(chr(13) + chr(10), chr(10))
        self.assertIn("{% if (fm_modo or 'ficha') == 'ficha' and permissions and permissions.superadmin %}\n  <template data-fm-extra>", motor)
        self.assertIn('onclick="otdFinCorrAbrir()"', motor)
        self.assertIn('href="/ot/finanzas-dudosas"', motor)
        self.assertNotIn('class="otd-fincorr-bar"', html)
        self.assertIn('id="otdModalFinCorr"', html)
        self.assertIn("async function otdFinCorrAbrir(){", html)


if __name__ == "__main__":
    unittest.main()

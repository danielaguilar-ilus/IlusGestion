"""Revisión adversarial del saldo por línea de servicio (08-oct-2026). Cada caso vigila una fuga real que encontró el
revisor: el TypeError de `codigo` duplicado en el asistente, el PUT genérico sin candado, la autorización cuyo id viene
del navegador, la garantía que seguía consumiendo saldo, Corregir finanzas sin reanotar, asociar-factura sobre una OT
cerrada y el JOIN que anulaba el índice. Sin BD, sin Flask, sin ERP (REGLA #4.1).
Correr con:  py -m unittest tests.test_ot_saldo_revision_0810
"""
import ast
import unittest

from tests.test_ot_saldo_servicio import _fuente, _arbol


def _exec(nombre, amb):
    _codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            n.decorator_list = []
            exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
            return amb[nombre]
    raise AssertionError("no está " + nombre)


class TestCodigoDuplicado(unittest.TestCase):
    def test_el_extra_del_validador_no_lleva_codigo_ni_http(self):
        src = _fuente("_ot_validar_normalizar_finanzas")
        self.assertIn('"ok", "codigo", "http"', src)

    def test_ot2_err_no_revienta_con_codigo_en_extra(self):
        amb = {"jsonify": lambda p: p}
        f = _exec("_ot2_err", amb)
        err = {"ok": False, "error": "x", "error_codigo": "ZZ_SALDO_CONSUMIDO", "codigo": "ZZ_SALDO_CONSUMIDO",
               "excesos": [1], "acciones": [2], "http": 409}
        extra = {k: v for k, v in err.items() if k not in ("error", "error_codigo", "ok", "codigo", "http")}
        cuerpo, http = f("msg", "ZZ_SALDO_CONSUMIDO", http=409, **extra)
        self.assertEqual(http, 409)
        self.assertEqual(cuerpo["codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(cuerpo["excesos"], [1])


class TestPutGenerico(unittest.TestCase):
    def test_el_put_pasa_por_el_candado_y_reanota(self):
        f = _fuente("mant_visita_update")
        self.assertIn("_ot_saldo_chequear(", f)
        self.assertIn("_ot_saldo_error_json(_sal_err_put, vid)", f)
        self.assertIn("_ot_saldo_reanotar_aportes(vid)", f)
        # el candado va ANTES del UPDATE
        self.assertLess(f.index("_ot_saldo_chequear("), f.index("UPDATE mant_visitas SET {','.join(sets)}"))


class TestAutorizacionDelNavegador(unittest.TestCase):
    def _mundo(self, fila, superadmin=False, uid=7, rut_aut="11.111.111-1"):
        consultas = []

        def fetchone(sql, params=()):
            consultas.append(sql)
            if "FROM mant_clientes" in sql:
                return {"rut": rut_aut}
            return fila

        amb = {"mysql_fetchone": fetchone, "mysql_fetchall": lambda *a, **k: [], "json": __import__("json"),
               "re": __import__("re"), "print": lambda *a, **k: None, "_ot_aut_usuario": lambda: (uid, "x"),
               "_ot_aut_es_superadmin": lambda: superadmin,
               "_saldo": __import__("ot_saldo_servicio")}
        return _exec("_ot_saldo_autorizado_hasta", amb)

    def _fila(self, **kw):
        base = {"payload_json": '{"saldo_pedido": {"servicio": 200000}}', "visita_id": None, "visita_creada_id": None,
                "solicitado_por_user_id": 7, "cliente_id": 3}
        base.update(kw)
        return base

    def test_la_propia_autorizacion_de_creacion_vale(self):
        f = self._mundo(self._fila())
        self.assertEqual(f(aut_id=1, cliente_rut="11111111-1")["servicio"], 200000)

    def test_la_del_superadmin_que_aprueba_vale(self):
        f = self._mundo(self._fila(solicitado_por_user_id=99), superadmin=True)
        self.assertEqual(f(aut_id=1)["servicio"], 200000)

    def test_la_de_otra_persona_no_vale(self):
        f = self._mundo(self._fila(solicitado_por_user_id=99))
        self.assertEqual(f(aut_id=1)["servicio"], 0)

    def test_la_de_la_ficha_de_una_ot_no_vale(self):
        f = self._mundo(self._fila(visita_id=55))
        self.assertEqual(f(aut_id=1)["servicio"], 0)

    def test_la_ya_consumida_no_vale(self):
        f = self._mundo(self._fila(visita_creada_id=60))
        self.assertEqual(f(aut_id=1)["servicio"], 0)

    def test_la_de_otro_cliente_no_vale(self):
        f = self._mundo(self._fila(), rut_aut="22.222.222-2")
        self.assertEqual(f(aut_id=1, cliente_rut="11.111.111-1")["servicio"], 0)

    def test_el_validador_pasa_el_rut_del_cliente(self):
        self.assertIn("aut_id=_fin_aut_id, cliente_rut=cliente_rut", _fuente("_ot_validar_normalizar_finanzas"))


class TestGarantiaNoConsumeSaldo(unittest.TestCase):
    def test_la_fuente_de_aportes_por_documento_excluye_garantia_y_cero(self):
        f = _fuente("_ot_saldo_usos")
        i = f.index("d.zz_serv_monto, d.zz_envio_monto")      # la primera consulta (aportes por documento)
        j = f.index("LIMIT %s", i)
        tramo = f[i:j]
        self.assertIn("NOT IN ('garantia','sin_costo')", tramo)
        self.assertIn("cobro_cero_motivo IS NULL", tramo)


class TestCorregirFinanzas(unittest.TestCase):
    def test_corregir_reanota_y_avisa(self):
        f = _fuente("ot2_api_finanzas_corregir")
        self.assertIn("_ot_saldo_reanotar_aportes(vid)", f)
        self.assertIn("aviso_saldo", f)


class TestAsociarFacturaCerrada(unittest.TestCase):
    def test_no_dice_tomo_saldo_si_no_bajo_nada(self):
        f = _fuente("mant_ot_asociar_factura")
        self.assertIn('"cerrada"', f)
        self.assertIn("_tomo_real_af", f)
        self.assertIn('not _b_ts.get("sin_cambios")', f)


class TestCompartidosSinFuncionEnElJoin(unittest.TestCase):
    def test_no_usa_trim_sobre_la_columna_indexada(self):
        f = _fuente("_ot_saldo_compartidos_lote")
        self.assertNotIn("TRIM(LEADING", f)
        self.assertIn("b.erp_nudo IN (", f)

    def test_cruza_con_ceros_a_la_izquierda(self):
        filas_a = [{"vid": 1, "erp_tido": "FCV", "erp_nudo": "0000011439"}]
        filas_b = [{"otra": 2, "erp_tido": "FCV", "erp_nudo": "11439", "numero_ot": "OT-2", "cliente": "C"}]
        llamadas = []

        def fetchall(sql, params=()):
            llamadas.append(sql)
            return filas_a if "SELECT visita_id AS vid" in sql else filas_b

        amb = {"mysql_fetchall": fetchall, "print": lambda *a, **k: None,
               "_ot_doc_real_a_usuario": lambda t, n: (t, n)}
        out = _exec("_ot_saldo_compartidos_lote", amb)([1])
        self.assertEqual(out[1][0]["otras"][0]["vid"], 2)
        self.assertEqual(len(llamadas), 2)


if __name__ == "__main__":
    unittest.main()

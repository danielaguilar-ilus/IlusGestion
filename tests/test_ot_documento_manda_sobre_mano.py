"""El documento MANDA sobre un cobro escrito a mano — 2026-10-29 (REGLA #56).

Daniel, con la OT-2026-00190 (cobro de $500.780 escrito a mano, sin origen ni motivo): «después que se agregó a mano, agregué una factura y no
lo reconoce como cobro por la factura… si yo sustituyo un valor a mano, ya no debería existir el valor a mano a menos que yo lo autorice… que
siempre predomine el valor del documento».

Se prueba la ruta REAL «agregar documento» (ot2_api_documentos_agregar) con una base y un ERP de mentira:
  · documento adicional sobre un cobro SIN origen / «escrito a mano» sin motivo → el documento reemplaza ese valor (con constancia);
  · el documento solo manda sobre la categoría de la que habla (si trae solo despacho, el servicio a mano NO se borra);
  · un cobro a mano CON motivo (autorizado por una persona) no se toca: el documento queda de referencia y se avisa;
  · un cobro que ya viene de documentos sigue SUMANDO como siempre, y el primer documento sigue fijando el valor;
  · un documento emitido a otra empresa no bloquea el cálculo de los demás en «Corregir finanzas».
"""
import datetime
import json
import re
import unittest

from tests import _fuente_app as FA


class _Mundo:
    def __init__(self, fila, aportes, lineas):
        self.fila = dict(fila)
        self.aportes = aportes            # lo que suman los documentos vivos DESPUÉS de insertar el nuevo
        self.lineas = lineas              # líneas ZZ del documento que se agrega
        self.sql, self.logs, self.llamadas_fin = [], [], 0

    def fetchone(self, sql, params=None):
        if "FROM mant_visitas v LEFT JOIN mant_clientes c" in sql:
            return dict(self.fila)
        if "SUM(zz_serv_monto)" in sql and "COUNT(*)" not in sql:
            return {"s": self.aportes[0], "e": self.aportes[1]}
        if "COUNT(*) AS n" in sql:
            return {"n": 0, "s": 0, "e": 0, "c": 0}
        if sql.startswith("SELECT id FROM mant_visita_documentos"):
            return None
        if "FROM mant_visitas v WHERE v.id=%s" in sql:                      # _OT_FIN_SELECT: antes / después
            self.llamadas_fin += 1
            f = dict(self.fila)
            if self.llamadas_fin > 1:                                      # lo que dejó el UPDATE
                for sql_u, par in self.sql:
                    if "UPDATE mant_visitas SET zz_monto=%s, zz_envio_monto=%s" in sql_u:
                        f["zz_monto"], f["zz_envio_monto"] = par[0], par[1]
                        if par[4]:
                            f["valor_origen"] = "zz"
            return f
        return None


def _fila(**kw):
    base = {"id": 190, "factura_tido": "FCV", "factura_nudo": "0000011262", "estado_facturacion": "facturado", "zz_monto": 500780,
            "zz_envio_monto": None, "costo": None, "zz_codigo": None, "valor_origen": "", "zz_motivo_manual": None,
            "cli_rut": "76.111.111-1", "razon_social": "Inmobiliaria y Constructora CF Ltda", "numero_ot": "OT-2026-00190"}
    base.update(kw)
    return base


def _correr(fila, lineas, aportes, primero=False):
    mundo = _Mundo(fila if not primero else dict(fila, factura_nudo="", factura_tido=""), aportes, lineas)

    class _Erp:
        TIDO_NUDO_MAP = {}

    class _Saldo:
        PREFIJOS_NOTA_VENTA = ("VD", "WEB")

    def ejecutar(sql, params=()):
        mundo.sql.append((sql, params))

    extra = {
        "re": re, "json": json, "datetime": datetime.datetime,     # en app.py `datetime` es la clase (from datetime import datetime)
        # confirmar_duplicado: el ERP de mentira devuelve las mismas líneas para todo documento y el freno de «posible duplicado» no es lo que se prueba acá.
        "request": type("R", (), {"get_json": staticmethod(
            lambda silent=True: {"origen": "erp", "tipo": "FCV", "numero": "11513", "confirmar_duplicado": True})})(),
        "jsonify": lambda d: d, "mysql_fetchone": mundo.fetchone, "mysql_fetchall": lambda sql, params=None: [],
        "mysql_execute": ejecutar, "mysql_execute_returning_rowcount": lambda sql, params=(): 0,
        "current_username": lambda: "daniel@sphs.cl", "_mant_log": lambda *a, **k: mundo.logs.append(a),
        "erp_engine": _Erp(), "_saldo": _Saldo(),
        "_OT_DOCS_CIERRE": ("FCV", "BLV", "FCE", "BLE", "NVV", "NVI"), "_OT_DOCS_COBRO": ("FCV", "BLV", "FCE", "BLE"),
        "_OT_DOCS_NOTA_VENTA": ("NVV", "NVI"),
        "_ot_resolver_doc_erp": lambda t, n, rut=None: ("FCV", {"cliente_rut": "76.111.111-1", "valor_bruto": 900000, "fecha": "2026-10-20"}, True, None),
        "_rut_analisis_comparacion": lambda a, b: {"match": True},
        "_erp_zz_lineas": lambda t, n: ({"ok": 1}, lineas, 900000),
        "_ot_saldo_chequear": lambda *a, **k: None, "_ot_saldo_autorizado_hasta": lambda **k: {"servicio": 0, "despacho": 0},
        "_ot_fin_costo_espejo_sync": lambda *a, **k: None, "_ot_docs_listar": lambda vid: {"documentos": []},
        "_ot_doc_registrar_fila": lambda *a, **k: None,
        "_OT_FIN_SELECT": "SELECT ... FROM mant_visitas v WHERE v.id=%s", "_ot_fin_rep_de": lambda vid: None,
        "_ot_fin_ganancia": lambda r, rep=None: 0,
    }
    amb = FA.cargar(funciones=["ot2_api_documentos_agregar", "_ot_doc_zz_plata", "_ot_doc_real_a_usuario", "_erp_fecha_a_date",
                               "_ot_fin_clp", "_ot_fin_registrar_automatico"], extra=extra)
    return amb["ot2_api_documentos_agregar"](190), mundo


LIN_SERV = [{"sku": "ZZINSTALACION", "monto": 500780}]
LIN_SERV_DESP = [{"sku": "ZZINSTALACION", "monto": 500780}, {"sku": "ZZENVIO", "monto": 100000}]
LIN_DESP = [{"sku": "ZZENVIO", "monto": 100000}]


def _update_valor(mundo):
    return [(s, p) for s, p in mundo.sql if "UPDATE mant_visitas SET zz_monto=%s, zz_envio_monto=%s" in s]


def _suma(mundo):
    return [(s, p) for s, p in mundo.sql if "zz_monto=COALESCE(zz_monto,0)+%s" in s]


class TestDocumentoMandaSobreMano(unittest.TestCase):
    def test_un_cobro_sin_origen_es_reemplazado_por_lo_que_aporta_el_documento(self):
        r, m = _correr(_fila(zz_monto=700000, valor_origen=""), LIN_SERV_DESP, (500780, 100000))
        self.assertTrue(r["ok"], r)
        up = _update_valor(m)
        self.assertEqual(len(up), 1)
        self.assertEqual(up[0][1][:2], (500780, 100000))                      # el valor de la factura, no el escrito a mano
        self.assertEqual(up[0][1][4], 1)                                       # y el origen pasa a «línea del documento»
        self.assertEqual(_suma(m), [], "no se suma encima del valor a mano")
        self.assertEqual(r["sumado"]["reemplazo_manual"], {"servicio": 700000, "despacho": 0})
        self.assertIsNone(r["aviso_cobro"])

    def test_deja_constancia_en_la_bitacora_y_en_el_registro_inmutable(self):
        r, m = _correr(_fila(zz_monto=700000, valor_origen=""), LIN_SERV_DESP, (500780, 100000))
        acciones = [a[2] for a in m.logs]
        self.assertIn("valor_a_mano_reemplazado", acciones)
        txt = [a[3] for a in m.logs if a[2] == "valor_a_mano_reemplazado"][0]
        self.assertIn("$700.000", txt)                                         # qué valor dejó de existir
        self.assertIn("$500.780", txt)
        self.assertIn("daniel@sphs.cl", txt)                                   # quién
        reg = [(s, p) for s, p in m.sql if "INSERT INTO mant_ot_finanzas_correcciones" in s]
        self.assertEqual(len(reg), 1)
        antes, despues = json.loads(reg[0][1][4]), json.loads(reg[0][1][5])
        self.assertEqual((antes["zz_monto"], despues["zz_monto"]), (700000, 500780))
        self.assertEqual(despues["valor_origen"], "zz")
        self.assertIn("REGLA #56", reg[0][1][3])

    def test_un_cobro_escrito_a_mano_sin_motivo_tambien_es_reemplazado(self):
        for origen in ("manual", "supuesto", "estimado"):
            r, m = _correr(_fila(valor_origen=origen, zz_motivo_manual=None), LIN_SERV, (500780, 0))
            self.assertEqual(len(_update_valor(m)), 1, origen)
            self.assertEqual(_suma(m), [], origen)

    def test_el_documento_solo_manda_sobre_la_categoria_de_la_que_habla(self):
        # La factura trae solo despacho: el servicio escrito a mano NO se borra (nadie decidió perder esa plata), queda a la vista sin respaldo.
        r, m = _correr(_fila(zz_monto=500780, zz_envio_monto=None, valor_origen=""), LIN_DESP, (0, 100000))
        up = _update_valor(m)
        self.assertEqual(up[0][1][:2], (500780, 100000))
        self.assertEqual(up[0][1][4], 0)                                       # el servicio sigue sin respaldo: el origen NO pasa a «documento»
        self.assertIn("servicio", r["aviso_cobro"])
        self.assertIn("sin documento de respaldo", r["aviso_cobro"])
        self.assertEqual(r["sumado"]["reemplazo_manual"], {"servicio": None, "despacho": 0})

    def test_un_cobro_a_mano_con_motivo_es_una_decision_de_una_persona_y_no_se_toca(self):
        r, m = _correr(_fila(valor_origen="manual", zz_motivo_manual="Concesión comercial acordada con el cliente"), LIN_SERV_DESP, (500780, 100000))
        self.assertTrue(r["ok"], r)
        self.assertEqual(_update_valor(m), [])
        self.assertEqual(_suma(m), [])                                         # ni reemplaza ni suma encima
        self.assertIsNone(r["sumado"])
        self.assertIn("escrito a mano con motivo", r["aviso_cobro"])
        self.assertIn("Tomar el cobro de los documentos", r["aviso_cobro"])
        ins = [p for s, p in m.sql if "INSERT INTO mant_visita_documentos" in s][0]
        self.assertEqual((ins[11], ins[12]), (None, None))                     # el documento no aportó nada: «quitar» después no resta de más
        self.assertIn("documento_no_toco_cobro_a_mano", [a[2] for a in m.logs])

    def test_un_cobro_que_ya_viene_de_documentos_sigue_sumando_como_siempre(self):
        r, m = _correr(_fila(zz_monto=300000, valor_origen="zz"), LIN_SERV, (800780, 0))
        self.assertEqual(len(_suma(m)), 1)
        self.assertEqual(_update_valor(m), [])
        self.assertNotIn("reemplazo_manual", r["sumado"])

    def test_el_primer_documento_sigue_fijando_el_valor(self):
        r, m = _correr(_fila(zz_monto=300000, valor_origen=""), LIN_SERV, (500780, 0), primero=True)
        self.assertTrue(r["es_principal"])
        up = [(s, p) for s, p in m.sql if "UPDATE mant_visitas SET zz_monto=%s, zz_envio_monto=%s" in s and "valor_origen='zz'" in s]
        self.assertEqual(len(up), 1)
        self.assertEqual(up[0][1][:2], (500780, 0))

    def test_sin_cobro_previo_el_documento_se_suma_como_siempre(self):
        r, m = _correr(_fila(zz_monto=None, valor_origen=""), LIN_SERV, (500780, 0))
        self.assertEqual(len(_suma(m)), 1)
        self.assertEqual(_update_valor(m), [])

    def test_si_el_documento_no_trae_lineas_de_servicio_no_toca_el_cobro(self):
        r, m = _correr(_fila(zz_monto=500780, valor_origen=""), [{"sku": "ILUS-PROD", "monto": 900000}], (0, 0))
        self.assertEqual(_update_valor(m), [])
        self.assertEqual(_suma(m), [])
        self.assertIsNone(r["sumado"])


class TestDocumentosDeOtraEmpresaNoBloquean(unittest.TestCase):
    def amb(self, saldo):
        extra = {"_ot_saldo_docs_de_ot": lambda vid: [("FCV", "11262"), ("BLV", "21654"), ("FCV", "11513")],
                 "_ot_saldo_de_docs": lambda d, excluir_vid=None, cliente_rut=None: saldo,
                 "mysql_fetchone": lambda sql, params=None: {"rut": "76.111.111-1"}}
        return FA.cargar(funciones=["_ot_fin_docs_cobro"], extra=extra)["_ot_fin_docs_cobro"]

    def test_la_boleta_de_otra_empresa_se_informa_pero_no_impide_tomar_el_cobro_de_la_factura_buena(self):
        cat = lambda t: {"hay_lineas": t > 0, "total": t, "usado": 0, "saldo": t}
        saldo = {"por_doc": [
            {"titulo": "FCV 11262", "leido": True, "otro_cliente": False, "omitido": None, "servicio": {"lineas": []}, "despacho": {"lineas": []}},
            {"titulo": "BLV 21654", "leido": True, "otro_cliente": True, "omitido": None, "servicio": {"lineas": []}, "despacho": {"lineas": []}},
            {"titulo": "FCV 11513", "leido": True, "otro_cliente": False, "omitido": None,
             "servicio": {"lineas": [{"sku": "ZZINSTALACION", "monto": 500780}]}, "despacho": {"lineas": []}}],
            "combinado": {"servicio": cat(500780), "despacho": cat(0)}}
        r = self.amb(saldo)(190, 5)
        self.assertFalse(r["incompleto"])                                      # antes: True, y la OT quedaba a mano
        self.assertEqual(r["otra_empresa"], ["BLV 21654"])
        self.assertEqual(r["servicio"]["saldo"], 500780)
        self.assertEqual(r["sku_servicio"], "ZZINSTALACION")

    def test_un_documento_que_random_no_entrego_sigue_bloqueando(self):
        saldo = {"por_doc": [{"titulo": "FCV 11513", "leido": False, "otro_cliente": False, "omitido": None,
                              "servicio": {"lineas": []}, "despacho": {"lineas": []}}],
                 "combinado": {"servicio": {"hay_lineas": False, "total": 0, "usado": 0, "saldo": 0},
                               "despacho": {"hay_lineas": False, "total": 0, "usado": 0, "saldo": 0}}}
        r = self.amb(saldo)(190, 5)
        self.assertTrue(r["incompleto"])
        self.assertEqual(r["otra_empresa"], [])


if __name__ == "__main__":
    unittest.main()

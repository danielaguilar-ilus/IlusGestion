# -*- coding: utf-8 -*-
"""🧹 Costeo de OT · Calidad de datos (FASE 2, Daniel 2026-10-29 — REGLA #59).

Daniel: «la calidad de datos, perfecto, pero SIEMPRE darle la prioridad al DOCUMENTO que lleva el cobro. Es muy probable que en una
factura quepan los productos y en otra solamente el servicio… también puede ser que DOS FACTURAS me tengan servicio y se puedan
SUMAR… y es muy importante identificar si hay cobros en otras OT para revisar el saldo».

Datos de MENTIRA (REGLA #22). La cuenta de cada OT es la REAL de app.py (`_ot_finanzas`, sacada por texto), los motivos de
«Finanzas dudosas» son los REALES (`_ot_fin_dudosas_motivos`) y la fila es la REAL del tablero Explorar (ot_costeo.fila). Se prueba:
  · las reglas puras con sus casos límite (dos facturas que se suman, una de productos + otra de servicio, aportes sin registrar,
    otra empresa, nota de venta ya facturada, garantía con documento, desinstalación cubierta, interna sin cliente, rojo solo con
    pérdida real);
  · el resumen, la concentración, la búsqueda, la paginación y los enlaces «Arreglar» (solo llevan a caminos que ya existen);
  · la ruta REAL de app.py con una base de mentira (403 a quien no es superadmin, solo SELECT, sin Random ni Check, caché);
  · la plantilla con sus tres pestañas y la pantalla ejecutada en Node con un DOM de mentira.

Correr con:  python -m unittest tests.test_ot_calidad
"""
import functools
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_anexo_costo as OAC  # noqa: E402
import ot_calidad as Q  # noqa: E402
import ot_costeo as C  # noqa: E402
import ot_gerencial as G  # noqa: E402
import ot_saldo_servicio as SALDO  # noqa: E402
from tests import _ger_app as A  # noqa: E402
from tests import _ger_demo as D  # noqa: E402

HOY = D.HOY
P = C.leer_parametros({"desde": "2026-09-01", "hasta": "2026-10-31"}, HOY)
JS = os.path.join(A.RAIZ, "static", "ot_costeo_calidad.js")
CSS = os.path.join(A.RAIZ, "static", "ot_costeo_calidad.css")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


def _amb_dudosas():
    amb = dict(A.cuenta_real())
    exec(compile(A.bloque(A.app_src(), "_ot_fin_dudosas_motivos"), "<app:_ot_fin_dudosas_motivos>", "exec"), amb)
    return amb


DUDOSAS = _amb_dudosas()["_ot_fin_dudosas_motivos"]


def doc(tido, nudo, serv=None, desp=None, es_cobro=1, reemplazado=None, heredado=None, rut_ok=1, justificado=0, origen="erp"):
    return {"origen": origen, "erp_tido": tido, "erp_nudo": nudo, "es_cobro": es_cobro, "reemplazado_por_id": reemplazado,
            "heredado_de_visita_id": heredado, "zz_serv_monto": serv, "zz_envio_monto": desp, "rut_ok": rut_ok,
            "justificado": justificado}


def caso(id, docs=(), auts=(), anexo=None, alertas=(), familias=None, cubre_cero=False, **kw):
    """(fila del tablero, ctx) de una OT de mentira, con la cuenta y los motivos de «Finanzas dudosas» REALES."""
    v = A.visita(id, **kw)
    fin = A.fin_de(v)
    g = G.armar_fila(v, fin, {"tipo_txt": "Instalación", "centro_txt": "Servicio Técnico", "intencion": v.get("intencion")})
    fila = C.fila(v, g, [], list(auts), P)
    ctx = {"v": v, "fin": fin, "docs": list(docs), "auts": list(auts), "cubre_cero": cubre_cero, "anexo": anexo,
           "dudosas": DUDOSAS(v, fin=fin), "alertas": list(alertas), "familias": familias or {}}
    return fila, ctx


def codigos(id, **kw):
    f, ctx = caso(id, **kw)
    return {h["codigo"]: h for h in Q.evaluar(f, ctx)}


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPrioridadDelDocumento(unittest.TestCase):
    """El cobro «oficial» es la SUMA de lo que aportan los documentos de cobro vivos."""

    def test_dos_facturas_con_servicio_se_suman(self):
        docs = [doc("FCV", "0000000201", serv=200000), doc("FCV", "0000000202", serv=150000)]
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", codigos(1, docs=docs, zz_monto=350000, costo_proveedor=200000))
        c = codigos(1, docs=docs, zz_monto=300000, costo_proveedor=200000)
        self.assertEqual(c["COBRO_DISTINTO_DE_DOCUMENTOS"]["monto"], 50000)
        self.assertIn("FCV 201: servicio $200.000", c["COBRO_DISTINTO_DE_DOCUMENTOS"]["detalle"])
        self.assertIn("aportan $350.000", c["COBRO_DISTINTO_DE_DOCUMENTOS"]["detalle"])

    def test_una_factura_solo_de_productos_y_otra_solo_de_servicio(self):
        docs = [doc("FCV", "301"), doc("FCV", "302", serv=300000)]          # la de productos no aportó (NULL)
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", codigos(2, docs=docs, zz_monto=300000, costo_proveedor=100000))
        c = codigos(2, docs=docs, zz_monto=250000, costo_proveedor=100000)  # declara MENOS de lo registrado: se puede afirmar
        self.assertEqual(c["COBRO_DISTINTO_DE_DOCUMENTOS"]["monto"], 50000)
        f, ctx = caso(2, docs=docs, zz_monto=400000, costo_proveedor=100000)
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", {h["codigo"] for h in Q.evaluar(f, ctx)},
                         "declara MÁS y hay un documento sin aporte registrado: la diferencia puede estar ahí → sin verificar")
        self.assertEqual(Q.verificacion(f, ctx), "sin_verificar")

    def test_aportes_sin_registrar_quedan_sin_verificar_nunca_se_inventa_un_cero(self):
        docs = [doc("FCV", "401"), doc("BLV", "402")]
        f, ctx = caso(3, docs=docs, zz_monto=300000, costo_proveedor=100000)
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", {h["codigo"] for h in Q.evaluar(f, ctx)})
        self.assertEqual(Q.verificacion(f, ctx), "sin_verificar")
        self.assertEqual(Q.resumen([Q.item(f, ctx)])["n_sin_verificar"], 1)
        self.assertEqual(Q.comparar_con_documentos(300000, Q.de_cobro(Q.normalizar_docs(docs)))["estado"], "sin_verificar")

    def test_un_documento_de_otra_empresa_no_cuenta(self):
        docs = [doc("FCV", "501", serv=200000), doc("BLV", "502", serv=100000, rut_ok=0)]
        c = codigos(4, docs=docs, zz_monto=300000, costo_proveedor=100000)
        self.assertEqual(c["COBRO_DISTINTO_DE_DOCUMENTOS"]["monto"], 100000, "la boleta de otro RUT no suma al cobro oficial")
        self.assertIn("DOC_OTRO_RUT", c)
        self.assertEqual(c["DOC_OTRO_RUT"]["monto"], 100000)
        docs[1]["justificado"] = 1
        c = codigos(4, docs=docs, zz_monto=300000, costo_proveedor=100000)
        self.assertNotIn("DOC_OTRO_RUT", c, "justificada ya no se acusa…")
        self.assertIn("COBRO_DISTINTO_DE_DOCUMENTOS", c, "…pero sigue sin contar como cobro (mismo criterio del saldo)")

    def test_la_nota_de_venta_dada_de_baja_y_la_copia_de_la_madre_no_cuentan(self):
        docs = [doc("NVV", "VD00010667", serv=300000, reemplazado=9), doc("FCV", "11348", serv=300000),
                doc("FCV", "999", serv=50000, heredado=7)]
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", codigos(5, docs=docs, zz_monto=300000, costo_proveedor=100000))

    def test_un_cobro_a_mano_con_motivo_es_decision_de_una_persona(self):
        docs = [doc("FCV", "601", serv=200000)]
        c = codigos(6, docs=docs, zz_monto=300000, costo_proveedor=100000, valor_origen="manual",
                    zz_motivo_manual="Acuerdo comercial con el cliente por la visita extra")
        self.assertNotIn("COBRO_DISTINTO_DE_DOCUMENTOS", c)
        self.assertNotIn("COBRO_SIN_ORIGEN", c)


class TestReglas(unittest.TestCase):
    def test_documento_declarado_y_cobro_en_blanco(self):
        c = codigos(10, docs=[doc("FCV", "11348")], zz_monto=None, zz_codigo=None, valor_origen=None, costo_proveedor=195000)
        self.assertIn("COBRO_DOC_SIN_DECLARAR", c)
        self.assertIsNone(c["COBRO_DOC_SIN_DECLARAR"]["monto"], "sin aporte registrado no hay monto que inventar")
        self.assertIn("FCV 11348", c["COBRO_DOC_SIN_DECLARAR"]["detalle"])
        for docs in ([doc("NVV", "VD1", reemplazado=3)], [doc("FCV", "1", rut_ok=0)], [doc("FCV", "1", heredado=2)],
                     [doc("FCV", "1", es_cobro=0)]):
            self.assertNotIn("COBRO_DOC_SIN_DECLARAR", codigos(10, docs=docs, zz_monto=None, valor_origen=None, costo_proveedor=1))
        self.assertNotIn("COBRO_DOC_SIN_DECLARAR", codigos(10, docs=[doc("FCV", "1")], zz_monto=0, costo_proveedor=1),
                         "un cobro declarado en $0 no está «en blanco» (igual que «Finanzas por reparar»)")

    def test_cobro_sin_origen(self):
        self.assertTrue(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": None}))
        self.assertTrue(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": "manual", "zz_motivo_manual": "  "}))
        self.assertTrue(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": "supuesto"}))
        self.assertFalse(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": "manual", "zz_motivo_manual": "Acuerdo"}))
        self.assertFalse(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": "zz"}))
        self.assertFalse(Q.cobro_sin_origen({"zz_monto": 300000, "valor_origen": "estimado"}))
        self.assertFalse(Q.cobro_sin_origen({"zz_monto": 1, "valor_origen": None, "zz_codigo": "ZZRETIRO"}))
        self.assertFalse(Q.cobro_sin_origen({"zz_monto": 0, "valor_origen": None}))
        self.assertEqual(Q.fuente_cobro_txt({"valor_origen": None}), "declarado")
        self.assertEqual(Q.fuente_cobro_txt({"valor_origen": "manual"}), "escrito a mano")
        c = codigos(11, zz_monto=300000, valor_origen=None, costo_proveedor=100000)
        self.assertEqual(c["COBRO_SIN_ORIGEN"]["monto"], 300000)
        self.assertNotIn("COBRO_SIN_ORIGEN", codigos(11, zz_monto=300000, valor_origen=None, costo_proveedor=1,
                                                     modalidad_cobro="garantia", cobro_cero_motivo="garantia"))

    def test_solo_nota_de_venta_y_nota_ya_facturada(self):
        nv = [doc("NVV", "VD00010667", serv=300000)]
        c = codigos(12, docs=nv, zz_monto=300000, costo_proveedor=100000, familias={("VD", "10667"): "FCV 11348"})
        self.assertIn("SOLO_NOTA_VENTA", c)
        self.assertIn("NV_YA_FACTURADA", c)
        self.assertIn("FCV 11348", c["NV_YA_FACTURADA"]["detalle"])
        abierta = codigos(12, docs=nv, zz_monto=300000, costo_proveedor=100000, estado="en_ejecucion")
        self.assertNotIn("SOLO_NOTA_VENTA", abierta, "para crear basta la nota de venta; la factura se exige al cerrar")
        con_fac = codigos(12, docs=nv + [doc("FCV", "0000011348", serv=300000)], zz_monto=600000, costo_proveedor=1,
                          familias={("VD", "10667"): "FCV 11348"})
        self.assertNotIn("NV_YA_FACTURADA", con_fac)
        self.assertNotIn("SOLO_NOTA_VENTA", con_fac)

    def test_garantia_con_documento(self):
        nv = [doc("NVV", "VD1", serv=0)]
        c = codigos(13, docs=nv, modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=90000,
                    valorizado_clp=50000, familias={("VD", "1"): "FCV 9"})
        for no in ("COBRO_DOC_SIN_DECLARAR", "SOLO_NOTA_VENTA", "NV_YA_FACTURADA", "COBRO_SIN_ORIGEN", "SIN_VALORIZAR"):
            self.assertNotIn(no, c, no + ": una garantía no pide factura (REGLA #56)")
        self.assertIn("CERO_SIN_AUTORIZACION", c)
        self.assertIn("SIN_INTENCION", c)
        self.assertEqual(c["COSTO_MAYOR_VALOR"]["monto"], 40000)
        ok = codigos(13, docs=nv, modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=40000,
                     valorizado_clp=50000, cubre_cero=True, intencion="garantia_fabrica")
        self.assertNotIn("CERO_SIN_AUTORIZACION", ok)
        self.assertNotIn("SIN_INTENCION", ok)
        self.assertNotIn("COSTO_MAYOR_VALOR", ok)

    def test_desinstalacion_cubierta(self):
        ax = {"numero": 77, "estado": "firmado", "rol": OAC.ROL_SALIDA,
              "items": [{"concepto": "Instalación", "monto": 100000}, {"concepto": "Desinstalación", "monto": 60000, "ot": "salida"}]}
        f, ctx = caso(14, tipo="desinstalacion", ot_relacion="desinstalacion", ot_madre_id=9, madre_valida=1, zz_monto=None,
                      zz_codigo=None, costo_proveedor=60000, anexo=ax)
        self.assertEqual(f["cob"], "cubierta")
        cods = {h["codigo"] for h in Q.evaluar(f, ctx)}
        for no in ("CERO_SIN_AUTORIZACION", "SIN_VALORIZAR", "COBRO_DOC_SIN_DECLARAR", "SIN_INTENCION", "ANEXO_DISTINTO"):
            self.assertNotIn(no, cods, no)
        f, ctx = caso(14, tipo="desinstalacion", ot_relacion="desinstalacion", ot_madre_id=9, madre_valida=1, zz_monto=None,
                      zz_codigo=None, costo_proveedor=50000, anexo=ax)
        h = {x["codigo"]: x for x in Q.evaluar(f, ctx)}
        self.assertEqual(h["ANEXO_DISTINTO"]["monto"], 10000, "se compara con SU parte del anexo compartido")

    def test_interna_sin_cliente(self):
        c = codigos(15, cliente_id=None, tipo="revision_interna", zz_monto=None, zz_codigo=None, costo_proveedor=None,
                    proveedor_tipo="interno", tecnico_role="tecnico", te_id=None, te_empresa=None)
        self.assertIn("SIN_VALORIZAR", c)
        for no in ("CERO_SIN_AUTORIZACION", "COBRO_DOC_SIN_DECLARAR", "TECNICO_SIN_COSTO", "MARGEN_100", "SIN_INTENCION"):
            self.assertNotIn(no, c, no)
        self.assertNotIn("SIN_VALORIZAR", codigos(15, cliente_id=None, tipo="revision_interna", zz_monto=None, valorizado_clp=80000,
                                                  proveedor_tipo="interno", tecnico_role="tecnico", te_id=None))

    def test_rojo_solo_con_perdida_real(self):
        self.assertEqual({c["codigo"] for c in Q.CODIGOS if c["severidad"] == "rojo"}, {"PERDIDA_SIN_AUTORIZACION"})
        c = codigos(16, zz_monto=100000, costo_proveedor=150000)
        self.assertEqual(c["PERDIDA_SIN_AUTORIZACION"]["monto"], 50000)
        aut = [{"tipo": "cerrar_con_perdida", "estado": "aprobada", "id": 3}]
        self.assertNotIn("PERDIDA_SIN_AUTORIZACION", codigos(16, zz_monto=100000, costo_proveedor=150000, auts=aut))
        self.assertNotIn("PERDIDA_SIN_AUTORIZACION", codigos(16, zz_monto=100000, costo_proveedor=150000, estado="en_ejecucion"))
        g = codigos(17, modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=90000, valorizado_clp=10000)
        self.assertFalse(any(Q.POR_CODIGO[k]["severidad"] == "rojo" for k in g), "una garantía cuesta, no «pierde»")
        self.assertEqual(set(Q.SEVERIDADES), {c["severidad"] for c in Q.CODIGOS})

    def test_tecnico_externo_sin_costo_y_margen_100(self):
        c = codigos(18, zz_monto=300000, costo_proveedor=None)
        self.assertIn("TECNICO_SIN_COSTO", c)
        self.assertNotIn("MARGEN_100", c, "la causa ya está dicha: no se cuenta dos veces")
        p = codigos(19, zz_monto=300000, costo_proveedor=0, proveedor_tipo="interno", tecnico_role="tecnico", te_id=None, te_empresa=None)
        self.assertEqual(p["MARGEN_100"]["monto"], 300000)
        self.assertNotIn("TECNICO_SIN_COSTO", p)

    def test_anexo_distinto_factura_sin_saldo_centro_y_tecnico(self):
        ax = {"numero": 158, "estado": "firmado", "rol": None, "items": [{"concepto": "Levantamiento", "monto": 30000}]}
        c = codigos(20, zz_monto=100000, costo_proveedor=20000, anexo=ax)
        self.assertEqual(c["ANEXO_DISTINTO"]["monto"], 10000)
        self.assertIn("Anexo N° 158", c["ANEXO_DISTINTO"]["detalle"])
        self.assertNotIn("ANEXO_DISTINTO", codigos(20, zz_monto=100000, costo_proveedor=30000, anexo=ax))
        al = [{"doc_tido": "FCV", "doc_nudo": "11348", "titulo": "FCV 11348", "tipo": "sin_saldo", "otras_ots": "OT-2026-00150",
               "estado": "vigente"}]
        c = codigos(21, zz_monto=100000, costo_proveedor=20000, alertas=al, centro_costo=None, tecnico_user_id=None,
                    tecnico_role=None, te_id=None, te_empresa=None, proveedor_tipo=None)
        self.assertIn("OT-2026-00150", c["FACTURA_SIN_SALDO"]["detalle"])
        self.assertIn("SIN_CENTRO_COSTO", c)
        self.assertIn("SIN_TECNICO", c)
        self.assertIsNone(c["SIN_TECNICO"]["monto"])

    def test_motivos_de_finanzas_dudosas_se_reutilizan(self):
        c = codigos(22, zz_monto=None, zz_codigo=None, valor_origen=None, costo=500000, costo_proveedor=100000)
        self.assertEqual(c["PRECIO_ANOTADO"]["monto"], 500000)
        z = codigos(23, zz_monto=1, zz_codigo="ZZRETIRO", costo_proveedor=1)
        self.assertEqual(z["ZZ_NO_SERVICIO"]["monto"], 1)
        s = codigos(24, zz_monto=None, zz_codigo=None, valor_origen="manual", zz_motivo_manual="Cobro sin línea, lo acordó Daniel",
                    costo=200000, costo_proveedor=50000)
        self.assertIn("SIN_SEPARAR", s)

    def test_el_orden_y_cada_codigo_dice_como_se_arregla(self):
        pedidos = {"COBRO_DOC_SIN_DECLARAR", "ANEXO_DISTINTO", "COBRO_SIN_ORIGEN", "SOLO_NOTA_VENTA", "NV_YA_FACTURADA",
                   "PERDIDA_SIN_AUTORIZACION", "CERO_SIN_AUTORIZACION", "TECNICO_SIN_COSTO", "MARGEN_100", "SIN_CENTRO_COSTO",
                   "SIN_VALORIZAR", "COSTO_MAYOR_VALOR", "FACTURA_SIN_SALDO", "DOC_OTRO_RUT", "PRECIO_ANOTADO", "SIN_SEPARAR",
                   "ZZ_NO_SERVICIO", "SIN_TECNICO", "SIN_INTENCION", "COBRO_DISTINTO_DE_DOCUMENTOS"}
        self.assertEqual(set(Q.ORDEN), pedidos)
        self.assertNotIn("CERRADA_SIN_MEMORIA", Q.POR_CODIGO, "fase 5")
        self.assertNotIn("HALLAZGO_POST_CIERRE", Q.POR_CODIGO, "fase 5")
        for c in Q.CODIGOS:
            self.assertGreater(len(c["regla"]), 30, c["codigo"])
            self.assertGreater(len(c["como"]), 20, c["codigo"])
        self.assertEqual([c["codigo"] for c in Q.CODIGOS if c["solo_cuenta"]], ["SIN_INTENCION"])


class TestCoherenciaConElProyecto(unittest.TestCase):
    def test_los_motivos_reutilizados_existen_tal_cual_en_app(self):
        fuente = A.bloque(A.app_src(), "_ot_fin_dudosas_motivos")
        for texto in Q.MOTIVOS_DUDOSAS:
            self.assertIn('"%s"' % texto, fuente, texto)

    def test_cuesta_mas_de_lo_que_vale_es_la_misma_regla_del_resumen(self):
        casos = [dict(modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=c, valorizado_clp=v)
                 for c, v in ((90000, 50000), (40000, 50000), (None, 10000), (50000, None))]
        casos += [dict(modalidad_cobro="sin_costo", costo_proveedor=80000, valorizado_clp=70000)]
        filas, mios = [], set()
        for i, kw in enumerate(casos, 300):
            f, ctx = caso(i, **kw)
            if "COSTO_MAYOR_VALOR" in {h["codigo"] for h in Q.evaluar(f, ctx)}:
                mios.add(i)
            v = A.visita(i, **kw)
            filas.append(G.armar_fila(v, A.fin_de(v), {}))
        del_resumen = {f["id"] for f in G.garantias(filas, filas, date(2026, 10, 31))["costo_mayor_valor"]}
        self.assertEqual(mios, del_resumen)
        self.assertEqual(mios, {300, 304})

    def test_el_documento_de_hoy_sale_de_la_misma_clasificacion_de_explorar(self):
        for t, esperado in (("FCV", "factura"), ("BLV", "boleta"), ("NVV", "nota_venta"), ("GDV", "otro_doc")):
            self.assertEqual(Q.normalizar_docs([doc(t, "1")])[0]["tipo"], C.tipo_doc({"origen": "erp", "tido": t}))
            self.assertEqual(Q.normalizar_docs([doc(t, "1")])[0]["tipo"], esperado)

    def test_el_principal_antiguo_cuenta_solo_si_se_valido(self):
        v = {"factura_tido": "FCV", "factura_nudo": "0000011348", "factura_asociada_por": "daniel"}
        self.assertEqual([d["titulo"] for d in Q.normalizar_docs([], v)], ["FCV 11348"])
        self.assertEqual(Q.normalizar_docs([], {"factura_tido": "FCV", "factura_nudo": "1"}), [])
        self.assertEqual(len(Q.normalizar_docs([doc("FCV", "11348")], v)), 1, "sin repetir el mismo documento")

    def test_modulo_puro(self):
        src = _leer(os.path.join(A.RAIZ, "ot_calidad.py"))
        for prohibido in ("import flask", "from flask", "mysql", "pymssql", "requests", "_random_sql", "_checkwms", "erp_engine"):
            self.assertNotIn(prohibido, src, prohibido)


def items_demo():
    """60 OT con el cobro sin respaldo (tres técnicos, 60 clientes, uno de nombre largo) y una en orden."""
    out = []
    for i in range(60):
        f, ctx = caso(1000 + i, zz_monto=300000 + i, valor_origen=None, costo_proveedor=100000, cliente_id=5000 + i,
                      tecnico_user_id=10 + i % 3, tecnico_nombre=["Ana Pérez", "José Núñez", "Luis Soto"][i % 3],
                      cliente="Gimnasio Ñuñoa Centro Deportivo Los Andes del Sur Limitada" if i == 55 else f"Cliente {i} SpA")
        out.append(Q.item(f, ctx))
    f, ctx = caso(2000, zz_monto=300000, costo_proveedor=100000)
    out.append(Q.item(f, ctx))
    return out


class TestResumenListaYArreglo(unittest.TestCase):
    def items(self):
        return items_demo()

    def test_resumen_cuenta_y_concentra(self):
        r = Q.resumen(self.items())
        self.assertEqual((r["n_ot"], r["n_con"], r["n_sin"]), (61, 60, 1))
        c = next(x for x in r["codigos"] if x["codigo"] == "COBRO_SIN_ORIGEN")
        self.assertEqual(c["n"], 60)
        self.assertEqual(c["monto"], sum(300000 + i for i in range(60)))
        self.assertEqual(c["severidad_txt"], "Revisar")
        tec = c["concentracion"]["tec"]
        self.assertEqual(sorted(t["n"] for t in tec), [20, 20, 20])
        cli = c["concentracion"]["cli"]
        self.assertEqual(cli[-1]["nombre"], "Otros 55")
        self.assertEqual(sum(x["n"] for x in cli), 60)
        self.assertEqual(r["por_severidad"]["ambar"], 60)
        self.assertEqual([x["codigo"] for x in r["codigos"]], list(Q.ORDEN), "todas las reglas viajan, también las sin casos")

    def test_lista_buscar_y_paginar_como_etiquetas(self):
        its = self.items()
        lista = Q.filtrar(its, "COBRO_SIN_ORIGEN")
        self.assertEqual(lista[0]["_monto"], 300059, "la de más plata primero")
        self.assertEqual(len(Q.filtrar(its, "COBRO_SIN_ORIGEN", "nunoa los andes")), 1, "sin tildes ni mayúsculas, todas las palabras")
        self.assertEqual(len(Q.filtrar(its, "COBRO_SIN_ORIGEN", "jose")), 20)
        p = Q.paginar(lista, 2, 25)
        self.assertEqual((p["desde"], p["hasta"], p["total"], p["pages"], p["page"]), (26, 50, 60, 3, 2))
        self.assertEqual(Q.paginar(lista, 99, 25)["page"], 3, "una página de más vuelve a la última")
        self.assertEqual(Q.paginar([], 1, 25)["desde"], 0)
        self.assertEqual(Q.leer_pagina({"page": "x", "por": "13"}), (1, 25))
        self.assertEqual(Q.leer_pagina({"page": "4", "por": "100"}), (4, 100))

    def test_arreglar_solo_lleva_a_caminos_que_ya_existen(self):
        it = {"id": 151, "numero": "OT-2026-00151", "cli": "5", "cli_n": "Gimnasio Uno SpA"}
        r = {"ot": "/ot/0", "reparar": "/ot/finanzas-reparar", "regularizar": "/ot/regularizar", "saldo": "/ot/saldo-facturas"}
        self.assertEqual(Q.arreglo("COBRO_DOC_SIN_DECLARAR", it, r)["url"], "/ot/finanzas-reparar?tab=cobro&q=OT-2026-00151")
        self.assertEqual(Q.arreglo("COBRO_SIN_ORIGEN", it, r)["url"], "/ot/finanzas-reparar?tab=origen&q=OT-2026-00151")
        self.assertEqual(Q.arreglo("ANEXO_DISTINTO", it, r)["url"], "/ot/finanzas-reparar?tab=anexo&q=OT-2026-00151")
        self.assertEqual(Q.arreglo("SOLO_NOTA_VENTA", it, r)["url"], "/ot/regularizar?falta=factura&cliente_q=Gimnasio+Uno+SpA")
        self.assertEqual(Q.arreglo("CERO_SIN_AUTORIZACION", it, r)["url"],
                         "/ot/regularizar?falta=autorizacion_cobro_cero&cliente_q=Gimnasio+Uno+SpA")
        self.assertEqual(Q.arreglo("FACTURA_SIN_SALDO", it, r)["url"], "/ot/saldo-facturas")
        self.assertEqual(Q.arreglo("MARGEN_100", it, r)["url"], "/ot/151#otdCardFinanzas")
        self.assertEqual(Q.arreglo("SIN_VALORIZAR", it, r)["url"], "/ot/151")
        self.assertIsNone(Q.arreglo("SIN_INTENCION", it, r), "solo se cuenta")
        regularizar = set(re.findall(r'"(\w+)": "', A.bloque(A.app_src(), "_OT_REG_FALTA_TXT")))
        self.assertTrue({"factura", "autorizacion_cobro_cero"} <= regularizar, "los filtros de Regularizar existen")

    def test_quien_mas_usa_cada_documento_segun_el_libro(self):
        movs = [{"id": 1, "doc_tido": "FCV", "doc_nudo": "11348", "categoria": "servicio", "visita_id": 150, "numero_ot": "OT-2026-00150",
                 "cliente": "Gimnasio Uno SpA", "acumulado_ot": 100000},
                {"id": 2, "doc_tido": "FCV", "doc_nudo": "0000011348", "categoria": "servicio", "visita_id": 151, "numero_ot": "OT-2026-00151",
                 "cliente": "Gimnasio Uno SpA", "acumulado_ot": 342857},
                {"id": 3, "doc_tido": "FCV", "doc_nudo": "11348", "categoria": "servicio", "visita_id": 149, "numero_ot": "OT-2026-00149",
                 "cliente": "X", "acumulado_ot": 50000},
                {"id": 4, "doc_tido": "FCV", "doc_nudo": "11348", "categoria": "servicio", "visita_id": 149, "numero_ot": "OT-2026-00149",
                 "cliente": "X", "acumulado_ot": 0}]
        u = Q.ultimos_usos(movs)
        self.assertEqual([x["numero"] for x in u[("FCV", "11348")]], ["OT-2026-00150", "OT-2026-00151"], "la 149 ya liberó")
        f, ctx = caso(151, docs=[doc("FCV", "0000011348", serv=342857)], zz_monto=342857, costo_proveedor=1)
        it = dict(Q.item(f, ctx), _monto=1, _detalle="x")
        fl = Q.fila_lista(it, "MARGEN_100", None, u, {("FCV", "11348"): {"cap_servicio": 442857, "usado_servicio": 442857,
                                                                            "cap_despacho": 0, "usado_despacho": 0, "n_ot": 2, "erp_leido": 1}})
        d = fl["docs"][0]
        self.assertEqual([x["numero"] for x in d["otras_ot"]], ["OT-2026-00150"], "la propia OT no se lista como «otra»")
        self.assertEqual(d["saldo"]["servicio"], 0)
        self.assertEqual(fl["url"], "/ot/151")


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def bloque_calidad():
    src = A.app_src()
    ini = src.index("#  🧹 COSTEO DE OT · CALIDAD DE DATOS")
    ini = src.rfind("# ═", 0, ini)
    fin = src.index("#  🔎 COSTEO DE OT · EXPLORAR")
    fin = src.rfind("# ═", 0, fin)
    return src[ini:fin]


def bloque_costeo():
    src = A.app_src()
    ini = src.rfind("# ═", 0, src.index("#  🔎 COSTEO DE OT · EXPLORAR"))
    fin = src.rfind("# ═", 0, src.index("#  📊 COSTEO DE ÓRDENES DE TRABAJO — el tablero gerencial"))
    return src[ini:fin]


class TestRuta(unittest.TestCase):
    """El bloque REAL de app.py (y los del costeo de los que toma la lectura) con una base de mentira."""

    @classmethod
    def setUpClass(cls):
        cls.bloque = bloque_calidad()

    def montar(self, es_superadmin=True, args=None):
        vs = [A.visita(1, zz_monto=300000, valor_origen=None, costo_proveedor=100000, cerrada_at=datetime(2026, 10, 20, 15)),
              A.visita(2, zz_monto=None, zz_codigo=None, valor_origen=None, costo_proveedor=100000, cerrada_at=datetime(2026, 10, 21, 15)),
              A.visita(3, modalidad_cobro="garantia", cobro_cero_motivo="garantia", costo_proveedor=50000,
                       cerrada_at=datetime(2026, 10, 22, 15))]
        docs = {2: [doc("FCV", "0000011348")], 1: [doc("FCV", "0000011348", serv=300000)]}
        cuenta = A.cuenta_real()

        def responder(sql, params):
            if "information_schema.COLUMNS" in sql:
                return []
            if "c.region AS cliente_region" in sql:
                ini, fin, limite = params
                return [dict(v) for v in vs if ini <= v["cerrada_at"] < fin][:limite]
            if "FROM mant_visita_documentos" in sql:
                return [dict(d, visita_id=vid, id=i) for i, (vid, ds) in enumerate(docs.items()) if vid in params for d in ds]
            if "FROM ot_fac_libro" in sql:
                return [{"id": 9, "doc_tido": "FCV", "doc_nudo": "11348", "categoria": "servicio", "visita_id": 150,
                         "numero_ot": "OT-2026-00150", "cliente": "Otro SpA", "acumulado_ot": 80000}]
            if any(t in sql for t in ("FROM mant_ot_autorizaciones", "FROM mant_anexos", "FROM ot_fac_doc", "FROM ot_fac_alerta",
                                      "cobro_cero_autorizacion_id FROM mant_visitas")):
                return []
            raise AssertionError("SQL no previsto: " + sql[:160])

        db = A.BaseFalsa(responder)

        class Resp(dict):
            def __init__(self, d):
                super().__init__(d)
                self.headers = {}

        dud = _amb_dudosas()
        amb = {
            "__name__": "app_bloque", "app": types.SimpleNamespace(route=lambda *a, **k: (lambda f: f)), "wraps": functools.wraps,
            "time": time, "io": io, "datetime": datetime, "timedelta": timedelta, "timezone": timezone, "date": date,
            "_mant_required": lambda f: f, "_no_tecnico": lambda f: f,
            "mysql_fetchall": db.fetchall, "mysql_fetchone": db.fetchone, "_ot_resp_ok": lambda: True,
            "_ot_fin_cols_sql": lambda alias="v": ", ".join(f"{alias}.{c}" for c in (
                "modalidad_cobro", "cubierto_por", "tipo", "cliente_id", "contrato_id", "costo", "zz_monto", "cobro_cero_motivo", "zz_codigo",
                "zz_envio_monto", "valor_origen", "costo_proveedor", "costo_despacho", "proveedor_tipo", "valorizado_clp", "valorizado_fuente",
                "zz_motivo_manual", "ot_madre_id", "ot_relacion")) + ", 0 AS contrato_real, 0 AS madre_valida",
            "_ot_fin_lote": lambda rows: {int(r["id"]): cuenta["_ot_finanzas"](r) for r in rows},
            "_ot_finanzas": cuenta["_ot_finanzas"], "_ot_fin_agregado": cuenta["_ot_fin_agregado"],
            "_ot_fin_dudosas_motivos": dud["_ot_fin_dudosas_motivos"],
            "_ot_fin_clasif_lote": lambda ids: {}, "_ot_intenciones_catalogo": lambda: [], "_ot_intencion_info": lambda *a, **k: None,
            "_now_chile": lambda: datetime(HOY.year, HOY.month, HOY.day, 15, 0, tzinfo=timezone(timedelta(hours=-3))),
            "_TIPO_OT_LABEL": {"instalacion": "Instalación"}, "_OT2_CENTROS_COSTO": (("sstt", "Servicio Técnico"),),
            "_ot_aut_es_superadmin": lambda: es_superadmin, "_friendly_error_page": lambda t, m, c: ("error", t, c),
            "request": types.SimpleNamespace(args=args or {}, path="/ot/api/costeo/calidad"),
            "flash": lambda *a, **k: None, "redirect": lambda u: ("redirect", u),
            "url_for": lambda e, **k: "/ot/0" if e == "ot2_detalle" else "/" + e,
            "jsonify": lambda d: Resp(d), "_oac": OAC, "_saldo": SALDO,
        }
        src = A.app_src()
        for nombre in ("_OT_COBRO_CERO_MOTIVOS", "_ot_aut_cubre_cero", "_ot_solo_superadmin"):
            exec(compile(A.bloque(src, nombre), "<app:%s>" % nombre, "exec"), amb)
        exec(compile(A.bloque_gerencial(), "<app:bloque_gerencial>", "exec"), amb)
        exec(compile(bloque_costeo(), "<app:bloque_costeo>", "exec"), amb)
        exec(compile(self.bloque, "<app:bloque_calidad>", "exec"), amb)
        return amb, db

    def test_rutas_decoradores_y_nombre(self):
        ruta = '@app.route("/ot/api/costeo/calidad", methods=["GET"])\n@_mant_required\n@_no_tecnico\n@_calidad_solo_superadmin\ndef ot_costeo_calidad('
        self.assertIn(ruta, self.bloque)
        self.assertEqual(self.bloque.count("@app.route("), 1)
        self.assertIn("return _ot_solo_superadmin(view)(*a, **kw)", self.bloque)
        conf = _leer(os.path.join(A.RAIZ, "tests", "test_confidencialidad_financiera_tecnicos.py"))
        self.assertIn("/ot/api/costeo", conf, "la prueba de confidencialidad atrapa la ruta")
        self.assertNotIn("costeo/calidad", bloque_costeo(), "el bloque de Explorar sigue con sus dos rutas")
        self.assertNotIn("costeo/calidad", A.bloque_gerencial())

    def test_solo_lectura_sin_random_ni_check_ni_ddl(self):
        sin_comentarios = "\n".join(l for l in self.bloque.split("\n") if not l.strip().startswith("#"))
        for verbo in ("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "TRUNCATE", "REPLACE", "GRANT", "MERGE"):
            self.assertIsNone(re.search(r"\b%s\b" % verbo, sin_comentarios), verbo)
        for llamada in ("mysql_execute", "conn.commit", ".commit(", "requests.", "_random_sql_query", "_checkwms_get", "erp_engine",
                        "fetch_document", "_send_ilus_email", "_mant_log", "_mant_erp_doc", "_erp_zz_lineas", "_ot_saldo_doc(",
                        "_ot_saldo_de_docs", "_ot_fin_docs_cobro"):
            self.assertNotIn(llamada, self.bloque, llamada)

    def test_un_tecnico_o_gestion_no_recibe_nada_ni_se_lee_la_base(self):
        amb, db = self.montar(es_superadmin=False)
        r = amb["ot_costeo_calidad"]()
        self.assertEqual(r[1], 403)
        self.assertFalse(r[0]["ok"])
        self.assertEqual(db.sqls, [])

    def test_el_superadmin_recibe_las_tarjetas_leyendo_solo_select(self):
        amb, db = self.montar()
        r = amb["ot_costeo_calidad"]()
        self.assertTrue(r["ok"], r)
        self.assertEqual(r.headers.get("Cache-Control"), "no-store")
        por = {c["codigo"]: c for c in r["codigos"]}
        self.assertEqual(por["COBRO_SIN_ORIGEN"]["n"], 1)
        self.assertEqual(por["COBRO_DOC_SIN_DECLARAR"]["n"], 1)
        self.assertEqual(por["CERO_SIN_AUTORIZACION"]["n"], 1)
        self.assertEqual(r["n_ot"], 3)
        self.assertTrue(all(s.split(" ", 1)[0].upper() == "SELECT" for s in db.sqls))
        self.assertLessEqual(len(db.sqls), 12, "por lotes: nada de una consulta por OT")
        for s in db.sqls:
            self.assertNotIn("2026-", s, "las fechas viajan como parámetros")

    def test_la_lista_de_un_codigo_paginada_con_quien_mas_usa_el_documento(self):
        amb, db = self.montar(args={"codigo": "cobro_sin_origen", "page": "1", "por": "10"})
        r = amb["ot_costeo_calidad"]()
        self.assertTrue(r["ok"], r)
        self.assertEqual((r["total"], r["page"], r["pages"], r["por"]), (1, 1, 1, 10))
        f = r["filas"][0]
        self.assertEqual(f["numero"], "OT-2026-00001")
        self.assertEqual(f["arreglo"]["url"], "/ot2_finanzas_reparar_pagina?tab=origen&q=OT-2026-00001")
        self.assertEqual(f["docs"][0]["otras_ot"][0]["numero"], "OT-2026-00150")
        self.assertTrue(any("FROM ot_fac_libro" in s for s in db.sqls))
        amb["request"].args = {"codigo": "NO_EXISTE"}
        r = amb["ot_costeo_calidad"]()
        self.assertEqual(r[1], 400)

    def test_la_cache_de_90_segundos(self):
        amb, db = self.montar()
        amb["ot_costeo_calidad"]()
        n = len(db.sqls)
        amb["ot_costeo_calidad"]()
        self.assertEqual(len(db.sqls), n)
        amb["request"].args = {"actualizar": "1"}
        amb["ot_costeo_calidad"]()
        self.assertGreater(len(db.sqls), n)
        self.assertEqual(amb["_CALIDAD_CACHE_SEG"], 90)

    def test_un_error_no_muestra_detalles_internos(self):
        amb, _ = self.montar()
        amb["_costeo_datos"] = lambda p, forzar=False: (_ for _ in ()).throw(RuntimeError("SELECT secreto FROM tabla"))
        r = amb["ot_costeo_calidad"]()
        self.assertEqual(r[1], 500)
        self.assertNotIn("SELECT", r[0]["error"])


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestPlantilla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import jinja2
        stub = ("<!doctype html><html><head>{% block head_extra %}{% endblock %}</head><body>"
                "{% block content %}{% endblock %}{% block scripts %}{% endblock %}</body></html>")
        env = jinja2.Environment(loader=jinja2.ChoiceLoader([jinja2.DictLoader({"base.html": stub}),
                                                             jinja2.FileSystemLoader(os.path.join(A.RAIZ, "templates"))]),
                                 autoescape=True)

        def url_for(endpoint, **kw):
            if endpoint == "static":
                return "/static/" + kw["filename"]
            if endpoint == "ot2_detalle":
                return "/ot/%s" % kw["vid"]
            return "/" + endpoint
        env.globals["url_for"] = url_for
        cls.html = env.get_template("ot2/gerencial.html").render(d=D.tablero_demo(), G=G, p={"modo": "semana", "dias": 60})

    def test_tres_pestanas_y_calidad_escondida(self):
        h = self.html
        self.assertLess(h.index('data-gc-pestana="explorar"'), h.index('data-gc-pestana="calidad"'))
        self.assertRegex(h, r'<section class="cx cq" id="cqApp" role="tabpanel" aria-labelledby="gcPestCalidad" hidden')
        self.assertIn('data-url="/ot_costeo_calidad"', h)
        self.assertLess(h.index("/static/ot_costeo_calidad.js"), h.index("/static/ot_costeo_explorar.js"),
                        "Calidad se carga antes: Explorar le avisa al abrir su pestaña")
        self.assertIn("/static/ot_costeo_calidad.css", h)

    def test_explorar_cambia_entre_las_tres_pestanas(self):
        js = _leer(os.path.join(A.RAIZ, "static", "ot_costeo_explorar.js"))
        self.assertIn("window.OtCosteoCalidad.mostrar(escribir)", js)
        self.assertIn("/^#calidad/.test(location.hash)", js)


class TestFuentesDeLaPantalla(unittest.TestCase):
    def setUp(self):
        self.js = _leer(JS)
        self.css = _leer(CSS)

    def test_reglas_del_proyecto(self):
        self.assertNotRegex(self.js, r"(?<![A-Za-z.])(alert|confirm|prompt)\(")                 # REGLA #1
        self.assertIn("Seleccionar/deseleccionar todo", self.js)                               # REGLA #14
        for t in ("Mostrando ", "Anterior", "Siguiente", "Página ", "Por página"):             # REGLA #4.3
            self.assertIn(t, self.js)
        self.assertNotRegex(self.css + self.js, r"text-overflow\s*:\s*ellipsis|line-clamp")    # REGLA #15
        self.assertIn("min-height:44px", self.css)                                               # REGLA #3

    def test_esta_pantalla_no_escribe_nada(self):
        self.assertNotRegex(self.js, r"method\s*:", "solo GET: «Arreglar» es un enlace")
        self.assertNotRegex(self.js, r"(?i)\b(post|put|delete|patch)\b['\"]")
        self.assertIn('<a class="cx-btn cq-arreglar" href="', self.js)


_HARNESS = r"""
const fs = require('fs');
const {jsPath, escenario} = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const api = require(jsPath);
const llamadas = [], copias = [], avisos = [];
let hash = escenario.hash || '';
const raiz = { innerHTML: '', _h: {}, attrs: {'data-url': '/ot/api/costeo/calidad', 'data-ot': '/ot/0'},
  getAttribute(n) { return this.attrs[n]; }, addEventListener(t, f) { (this._h[t] = this._h[t] || []).push(f); },
  contains() { return true; }, classList: { toggle() {} } };
function servidor(url, opts) {
  llamadas.push({url, metodo: (opts && opts.method) || 'GET'});
  const q = new URLSearchParams(url.split('?')[1] || '');
  if (!q.get('codigo')) return escenario.resumen;
  let filas = escenario.filas.filter(f => !q.get('q') || (f.numero + ' ' + f.cliente).toLowerCase().indexOf(q.get('q').toLowerCase()) >= 0);
  const por = +q.get('por') || 25, total = filas.length, pages = Math.max(1, Math.ceil(total / por));
  const page = Math.min(Math.max(1, +q.get('page') || 1), pages), ini = (page - 1) * por;
  const vista = filas.slice(ini, ini + por);
  return {ok: true, codigo: escenario.codigo, filas: vista, total, page, pages, por, desde: total ? ini + 1 : 0, hasta: ini + vista.length,
          concentracion: {tec: [{clave: 'u:1', nombre: 'Ana Pérez', n: 3, monto: 900000}]}, meta: {}};
}
const ctl = api.crear(raiz, {
  fetch: async (url, opts) => ({ok: true, json: async () => servidor(url, opts)}),
  copiar: async (t) => { copias.push(t); }, toast: (m, t) => avisos.push([m, t]),
  leerHash: () => hash, escribirHash: (h) => { hash = h; }
});
const espera = () => new Promise(r => setTimeout(r, 20));
function boton(attrs) {
  const b = {disabled: !!attrs.disabled, getAttribute: n => attrs[n] === undefined ? null : attrs[n]};
  b.closest = (s) => (s === '[data-cq-acc]' && attrs['data-cq-acc']) ? b : (s === '[data-cq-form]' && attrs['data-cq-form'] ? b : null);
  return b;
}
async function disparar(tipo, ev) { for (const f of (raiz._h[tipo] || [])) f(ev); await espera(); }
(async () => {
  await ctl.mostrar(false); await espera();
  const salida = [];
  const foto = (n) => salida.push({n, html: raiz.innerHTML, hash, llamadas: llamadas.length});
  foto('inicio');
  for (const p of escenario.pasos) {
    if (p.clic) await disparar('click', {target: boton(p.clic)});
    if (p.cambio) await disparar('change', {target: Object.assign(boton(p.cambio.attrs), {value: p.cambio.value, checked: p.cambio.checked})});
    if (p.buscar !== undefined) {
      const f = boton({'data-cq-form': 'buscar'}); f.elements = {q: {value: p.buscar}};
      await disparar('submit', {target: f, preventDefault() {}});
    }
    if (p.vaciar) await disparar('input', {target: {name: 'q', value: ''}});
    foto(p.n || '');
  }
  console.log(JSON.stringify({salida, llamadas, copias, avisos}));
})().catch(e => { console.error(e.stack || e); process.exit(1); });
"""


class TestPantallaEjecutada(unittest.TestCase):
    """ot_costeo_calidad.js corriendo en Node con un DOM y un servidor de mentira."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")

    def correr(self, pasos, hash_=""):
        if not self.node:
            self.skipTest("Node no está instalado")
        its = items_demo()
        resumen = dict(Q.resumen(its), ok=True, meta={"desde": "2026-09-01", "hasta": "2026-10-31", "generado": "29/10/2026 10:00"})
        filas = [Q.fila_lista(it, "COBRO_SIN_ORIGEN") for it in Q.filtrar(its, "COBRO_SIN_ORIGEN")]
        esc = {"resumen": resumen, "filas": filas, "codigo": Q.ficha_codigo("COBRO_SIN_ORIGEN"), "pasos": pasos, "hash": hash_}
        with tempfile.TemporaryDirectory() as d:
            arch, h = os.path.join(d, "datos.json"), os.path.join(d, "h.js")
            with open(arch, "w", encoding="utf-8") as fh:
                json.dump({"jsPath": JS, "escenario": esc}, fh)
            with open(h, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS)
            r = subprocess.run([self.node, h, arch], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr)
            return json.loads(r.stdout.strip().splitlines()[-1])

    def test_tarjetas_lista_paginacion_y_arreglar_solo_navega(self):
        tarjeta = {"data-cq-acc": "codigo", "data-val": "COBRO_SIN_ORIGEN"}
        out = self.correr([{"n": "abrir", "clic": tarjeta}, {"n": "sig", "clic": {"data-cq-acc": "pag", "data-val": "1"}},
                           {"n": "pp", "cambio": {"attrs": {"data-cq-cambio": "pp"}, "value": "50"}},
                           {"n": "cerrar", "clic": tarjeta}])
        s = {x["n"]: x for x in out["salida"]}
        self.assertIn("Cobro sin respaldo", s["inicio"]["html"])
        self.assertIn("61</b> OT revisadas", s["inicio"]["html"])
        self.assertNotIn("Mostrando", s["inicio"]["html"], "la lista se abre al tocar una tarjeta")
        a = s["abrir"]["html"]
        self.assertIn("Mostrando 1–25 de 60", a)
        self.assertIn("Página 1 de 3", a)
        self.assertIn("Gimnasio Ñuñoa Centro Deportivo Los Andes del Sur Limitada", a, "nombre completo (REGLA #15)")
        self.assertIn('<a class="cx-btn cq-arreglar" href="/ot/finanzas-reparar?tab=origen&amp;q=OT-2026-01059"', a)
        self.assertNotRegex(a, r'cq-arreglar"[^>]*data-cq-acc', "«Arreglar» no tiene acción de JS: es un enlace")
        self.assertIn("¿Dónde se concentra?", a)
        self.assertIn("#calidad?", s["abrir"]["hash"])
        self.assertIn("c=COBRO_SIN_ORIGEN", s["abrir"]["hash"])
        self.assertIn("Mostrando 26–50 de 60", s["sig"]["html"])
        self.assertIn("Mostrando 1–50 de 60", s["pp"]["html"])
        self.assertNotIn("Mostrando", s["cerrar"]["html"], "otro toque en la misma tarjeta la cierra")
        self.assertTrue(all(l["metodo"] == "GET" for l in out["llamadas"]), "esta pantalla solo lee")
        self.assertIn("page=2", out["llamadas"][2]["url"])
        self.assertIn("por=50", out["llamadas"][3]["url"])

    def test_seleccionar_todo_es_un_interruptor_y_copiar_no_escribe(self):
        out = self.correr([{"n": "todo", "clic": {"data-cq-acc": "sel-todo"}}, {"n": "copiar", "clic": {"data-cq-acc": "copiar"}},
                           {"n": "nada", "clic": {"data-cq-acc": "sel-todo"}}], hash_="#calidad?c=COBRO_SIN_ORIGEN")
        s = {x["n"]: x for x in out["salida"]}
        self.assertEqual(s["todo"]["html"].count('class="cx-chk" data-cq-cambio="sel"'), 25)
        self.assertEqual(s["todo"]["html"].count(" checked aria-label"), 25)
        self.assertIn("Copiar los 25 N° de OT elegidos", s["todo"]["html"])
        self.assertEqual(s["nada"]["html"].count(" checked aria-label"), 0, "la misma acción desmarca todo")
        self.assertEqual(len(out["copias"]), 1)
        self.assertEqual(len(out["copias"][0].split("\n")), 25)
        self.assertTrue(all(l["metodo"] == "GET" for l in out["llamadas"]))

    def test_buscar_y_al_vaciar_la_busqueda_se_recarga(self):
        out = self.correr([{"n": "buscar", "buscar": "Ñuñoa"}, {"n": "vaciar", "vaciar": True}], hash_="#calidad?c=COBRO_SIN_ORIGEN")
        s = {x["n"]: x for x in out["salida"]}
        self.assertIn("Mostrando 1–1 de 1", s["buscar"]["html"])
        self.assertIn("q=%C3%91u%C3%B1oa", out["llamadas"][-2]["url"])
        self.assertIn("Mostrando 1–25 de 60", s["vaciar"]["html"], "REGLA #4.3: al limpiar el filtro la lista se recarga")
        self.assertNotIn("q=", out["llamadas"][-1]["url"])

    def test_el_estado_vive_en_el_enlace(self):
        if not self.node:
            self.skipTest("Node no está instalado")
        js = ("const a=require(%s);const e=a.hashAEstado('#calidad?d=2026-10-01&h=2026-10-31&fe=creacion&al=todas&c=MARGEN_100&p=3&pp=50&q=Ana');"
              "const m=a.hashAEstado('#calidad?fe=hackeo&c=<b>&pp=7&d=ayer');"
              "console.log(JSON.stringify({e,m,h:a.estadoAHash(e),x:a.hashAEstado('#explorar')}));") % json.dumps(JS)
        r = subprocess.run([self.node, "-e", js], capture_output=True, text=True, encoding="utf-8", timeout=30)
        self.assertEqual(r.returncode, 0, r.stderr)
        o = json.loads(r.stdout)
        self.assertEqual((o["e"]["fe"], o["e"]["al"], o["e"]["c"], o["e"]["p"], o["e"]["pp"], o["e"]["q"]),
                         ("creacion", "todas", "MARGEN_100", 3, 50, "Ana"))
        self.assertEqual((o["m"]["fe"], o["m"]["c"], o["m"]["pp"], o["m"]["d"]), ("cierre", "", 25, ""), "nada se confía del enlace")
        self.assertIsNone(o["x"])
        self.assertTrue(o["h"].startswith("#calidad?d=2026-10-01"))


if __name__ == "__main__":
    unittest.main()

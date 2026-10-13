# -*- coding: utf-8 -*-
"""⚖️ 2026-10-13 — El lote de Facturación de proveedor sigue el costo corregido en la OT.

Caso real (Daniel, OT-2026-00264 de Daniel Pulgar / DAP Servicio): se asignó al lote #18 por $65.000 el 08/10; el 09/10
se corrigió el costo de la OT a $20.000 y el anexo N° 213 se firmó por $20.000, pero el lote siguió pagando $65.000
(«¿por qué no me reconoce que cambiaron los costos…?»). Ahora:
  · Si el lote PENDIENTE pagaba exactamente lo que la OT declaraba (la seguía), al corregir el costo en la OT el lote
    toma el monto nuevo, con constancia en las dos bitácoras, y el total del lote se pone al día si no tiene factura.
  · Si alguien escribió a mano otro monto al asignar, no se pisa: queda «≠ declarado» con el botón «Usar el costo de
    la OT» (decisión de una persona, con su nombre).
  · Un lote pagado o anulado nunca se toca, ni se deja una OT en $0 dentro de un lote.
Todo con datos de mentira (nunca contra producción).
"""
import types
import unittest

from tests.test_fac_libro_saldo import _exec
from tests.test_ot_saldo_servicio import _fuente, _leer


def _clp(n):
    return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")


class TestAplicarCosto(unittest.TestCase):
    def _amb(self, fila):
        self.sql, self.logs, self.al_dia = [], [], []
        amb = {"mysql_fetchone": lambda sql, p=None: fila,
               "mysql_execute_returning_rowcount": lambda sql, p=None: (self.sql.append((sql, p)) or 1),
               "_mant_log": lambda ent, eid, acc, det: self.logs.append((ent, eid, acc, det)),
               "_mfp_monto_lote_al_dia": lambda fid, f=None, cambio=None: (self.al_dia.append((fid, cambio))
                                                                           or {"recalculado": True, "aviso": "ok"}),
               "_mfp_clp_txt": _clp}
        return _exec("_mfp_aplicar_costo_ot", amb)

    def _fila(self, **kw):
        f = {"id": 7, "fid": 18, "monto": 65000, "estado_pago": "pendiente", "numero_oc": None,
             "numero_ot": "OT-2026-00264"}
        f.update(kw)
        return f

    def test_caso_real_65_a_20(self):
        r = self._amb(self._fila())(441, 20000, quien="victor")
        self.assertEqual((r["antes"], r["nuevo"], r["fid"]), (65000, 20000, 18))
        sql, p = self.sql[0]
        self.assertIn("SET fpi.monto=%s", sql)
        self.assertIn("fp.estado_pago='pendiente'", sql)          # nunca un lote pagado
        self.assertEqual(p, (20000, 7, 65000))                       # solo si sigue en el monto que se leyó
        self.assertEqual([l[2] for l in self.logs], ["monto_ot_actualizado", "factura_proveedor_monto_actualizado"])
        self.assertIn("pasó de $65.000 a $20.000", self.logs[0][3])
        self.assertEqual(self.al_dia, [(18, ("actualizó", "OT-2026-00264"))])

    def test_lote_pagado_no_se_toca(self):
        self.assertIsNone(self._amb(self._fila(estado_pago="pagada"))(441, 20000))
        self.assertEqual(self.sql, [])

    def test_no_deja_una_ot_en_cero_ni_reescribe_lo_mismo(self):
        self.assertIsNone(self._amb(self._fila())(441, 0))
        self.assertIsNone(self._amb(self._fila())(441, 65000))
        self.assertEqual(self.sql, [])

    def test_avisa_si_la_oc_ya_estaba_anotada(self):
        self._amb(self._fila(numero_oc="4500123"))(441, 20000)
        self.assertIn("la OC N° 4500123 ya estaba anotada", self.logs[0][3])


class TestTrasEscritura(unittest.TestCase):
    def _correr(self, previo, ahora):
        self.aplicados = []
        resp = types.SimpleNamespace(status_code=200)
        amb = {"g": types.SimpleNamespace(_mfp_costo_previo=previo), "print": lambda *a, **k: None,
               "_mfp_sugerido_ot": lambda vid: ahora,
               "current_username": lambda: "victor",
               "_mfp_aplicar_costo_ot": lambda vid, nuevo, quien=None, motivo="": self.aplicados.append((vid, nuevo, quien))}
        f = _exec("_mfp_costo_tras_escritura", amb)
        self.assertIs(f(resp), resp)
        return self.aplicados

    def test_el_lote_seguia_a_la_ot_y_toma_el_costo_nuevo(self):
        self.assertEqual(self._correr({441: {"monto": 65000, "sugerido": 65000}}, 20000), [(441, 20000, "victor")])

    def test_monto_escrito_a_mano_al_asignar_no_se_pisa(self):
        self.assertEqual(self._correr({441: {"monto": 50000, "sugerido": 65000}}, 20000), [])

    def test_si_el_costo_no_cambio_no_hace_nada(self):
        self.assertEqual(self._correr({441: {"monto": 65000, "sugerido": 65000}}, 65000), [])

    def test_sin_costo_declarado_no_se_inventa(self):
        self.assertEqual(self._correr({441: {"monto": 65000, "sugerido": 65000}}, None), [])


class TestCableado(unittest.TestCase):
    def test_antes_de_escribir_se_anota_solo_en_lotes_pendientes(self):
        f = _fuente("_mfp_costo_previo")
        self.assertIn("fp.estado_pago='pendiente'", f)
        self.assertIn('va.get("vid") or va.get("visita_id")', f)

    def test_boton_con_permisos_y_solo_lote_pendiente(self):
        f = _fuente("mant_factura_proveedor_usar_costo_ot")
        self.assertIn("if not _facprov_puede():", f)
        self.assertIn('if f.get("estado_pago") != "pendiente":', f)
        h = _leer("templates/mantenciones/factura_proveedor_detalle.html")
        self.assertIn("{% if pendiente and not it.sin_costo and it.sugerido > 0 %}", h)
        self.assertIn("async function fpdUsarCostoOt(vid, numero, antes, nuevo){", h)
        self.assertIn("ilusConfirm(", h[h.index("async function fpdUsarCostoOt"):h.index("async function fpdAsignar(vid)")])


if __name__ == "__main__":
    unittest.main()

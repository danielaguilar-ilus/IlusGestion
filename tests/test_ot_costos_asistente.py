"""🧾 Paso Costos del asistente de crear OT (Daniel, 12-oct-2026).

Daniel: «me tienes que indicar si ya hay una factura que trae el saldo y tener la opción de agregar más facturas… si
hay una factura anterior y me quedan 30.000 pesos, solamente puedo marcar hasta 30.000… si no, se solicitará
autorización… tendrás que tener un texto abajo indicando cuál es el saldo total de la factura… aunque yo mueva el
campo, tú deberás tener en la memoria cuánto he utilizado… la casilla del margen va a ser muy dinámica». Y la regla:
«cambio a mano que mueve el margen +10%» pide argumento. «Deja esa súper tabla para el superadministrador».

Qué se vigila:
  · El reparto entre facturas va PRIMERO hasta el saldo de cada una (error real corregido: con A de $30.000 de saldo
    y B libre, un cobro de $80.000 quedaba entero contra A).
  · La regla del 10 % (pura), su uso en el servidor (con salvaguarda: si el bloque de documentos no corre, el motivo
    se pide como antes) y la MISMA cuenta en el navegador (se ejecuta el JS real con Node).
  · Bajo «Le cobras»: saldo de cada factura, tope sin autorización, cuánto quedaría y «Agregar otra factura».
  · La tabla detallada (qué OT y clientes usan cada factura) es solo del superadministrador.
  · La cuenta dice: Le cobras al cliente · Saldo de sus facturas en Random · Le pagas al proveedor · Margen.
Correr con:  py -m unittest tests.test_ot_costos_asistente
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import ot_saldo_servicio as S
from tests.test_ot_saldo_servicio import _fuente, _leer

H = _leer("templates/ot2/_modal_crear.html")


class TestRepartoPorSaldo(unittest.TestCase):
    def test_el_ejemplo_del_error(self):
        saldo = [{"servicio": 30000, "despacho": 0}, {"servicio": 100000, "despacho": 0}]
        cap = [{"servicio": 100000, "despacho": 0}, {"servicio": 100000, "despacho": 0}]
        r = S.repartir_por_saldo(saldo, cap, 80000, 0)
        self.assertEqual([x["servicio"] for x in r], [30000, 50000])
        # Antes (por capacidad) quedaba entero contra la primera:
        self.assertEqual([x["servicio"] for x in S.repartir_aportes(cap, 80000, 0)], [80000, 0])

    def test_sin_otras_ot_es_igual_que_antes(self):
        cap = [{"servicio": 100000, "despacho": 20000}, {"servicio": 50000, "despacho": 0}]
        self.assertEqual(S.repartir_por_saldo(cap, cap, 120000, 15000), S.repartir_aportes(cap, 120000, 15000))

    def test_con_autorizacion_lo_que_sobra_va_hasta_la_capacidad(self):
        saldo = [{"servicio": 30000, "despacho": 0}]
        cap = [{"servicio": 100000, "despacho": 0}]
        self.assertEqual(S.repartir_por_saldo(saldo, cap, 50000, 0), [{"servicio": 50000, "despacho": 0}])


class TestReglaDel10(unittest.TestCase):
    def test_tomar_todo_el_saldo_no_pide(self):
        self.assertFalse(S.ajuste_margen(100000, 100000)["pide_argumento"])

    def test_hasta_10_puntos_no_pide_y_mas_si(self):
        self.assertFalse(S.ajuste_margen(100000, 90000)["pide_argumento"])
        a = S.ajuste_margen(100000, 85000)
        self.assertTrue(a["pide_argumento"])
        self.assertEqual(a["puntos"], 15.0)

    def test_sin_facturas_con_saldo_siempre_pide(self):
        self.assertTrue(S.ajuste_margen(0, 50000)["pide_argumento"])
        self.assertFalse(S.ajuste_margen(0, 0)["pide_argumento"])


class TestServidor(unittest.TestCase):
    def test_reparto_por_saldo_y_regla_del_10(self):
        f = _fuente("_ot_validar_normalizar_finanzas")
        self.assertIn("_saldo.repartir_por_saldo(_caps_saldo, _docs_t, _fin_zzm, _fin_zz_envio_m)", f)
        self.assertIn('"FINANZAS_AJUSTE_SIN_ARGUMENTO"', f)
        self.assertIn('_fin_motivo_diferido = bool(_fin_valor_origen == "manual"', f)
        # salvaguarda: si el bloque de documentos no corrió, el motivo se exige como antes
        self.assertIn("if (not es_interna and _fin_motivo_diferido and not _fin_ajuste_evaluado and not _fin_zz_motivo_manual):", f)
        # un supuesto (sin referencia) sigue pidiendo motivo siempre
        self.assertIn("and not _fin_zz_motivo_manual and not _fin_motivo_diferido):", f)


def _js_funcion(nombre):
    i = H.index("function %s(" % nombre)
    prof, j = 0, H.index("{", i)
    while True:
        if H[j] == "{":
            prof += 1
        elif H[j] == "}":
            prof -= 1
            if prof == 0:
                return H[i:j + 1]
        j += 1


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestMismaCuentaEnElNavegador(unittest.TestCase):
    def _ajuste_js(self, saldo_s, saldo_d, cobro_s, cobro_d):
        prog = "\n".join([_js_funcion("_o2mFinSaldoFacturas"), _js_funcion("_o2mFinCobroTotal"), _js_funcion("_o2mFinAjuste"),
                          "var S = %s;" % json.dumps({"fin_saldo": {"servicio": {"hay": True, "saldo": saldo_s},
                                                                     "despacho": {"hay": saldo_d is not None, "saldo": saldo_d or 0}},
                                                      "fin_zz_monto": cobro_s, "fin_zz_envio": {"monto": cobro_d}}),
                          "console.log(JSON.stringify(_o2mFinAjuste()));"])
        f = os.path.join(tempfile.gettempdir(), "o2m_ajuste.js")
        open(f, "w", encoding="utf-8").write(prog)
        return json.loads(subprocess.run(["node", f], capture_output=True, text=True, check=True).stdout)

    def test_igual_que_el_servidor(self):
        for ss, sd, cs, cd in ((100000, None, 100000, 0), (100000, None, 85000, 0), (100000, 20000, 110000, 20000),
                               (100000, 20000, 60000, 0), (0, None, 50000, 0)):
            js = self._ajuste_js(ss, sd, cs, cd)
            py = S.ajuste_margen(ss + (sd or 0), cs + cd)
            self.assertEqual(js["pide"], py["pide_argumento"], (ss, sd, cs, cd))
            self.assertEqual(js["puntos"], py["puntos"], (ss, sd, cs, cd))


class TestPantalla(unittest.TestCase):
    def test_saldo_bajo_le_cobras_con_tope_y_agregar_factura(self):
        self.assertIn("'<div id=\"o2mFinSaldoTxt\"></div>'+", H)
        f = _js_funcion("_pintarFinSaldoTxtDom")
        self.assertIn("ya usado por otras OT", f)
        self.assertIn("Puedes cobrar hasta", f)
        self.assertIn("Te pasas del saldo por", f)
        self.assertIn("OT2C.abrirAgregarDocExtra()", f)

    def test_motivo_solo_si_mueve_mas_de_10_puntos(self):
        self.assertIn("(_o2mFinEsManual() && (S.fin_valor_origen === 'supuesto' || _o2mFinAjusteGrande()))",
                      _js_funcion("_o2mFinPideMotivo"))
        self.assertIn("_o2mFinEnvioEsManual() && _o2mFinAjusteGrande() && !(S.fin_zz_envio_motivo_manual||'').trim()", H)

    def test_tabla_detallada_solo_superadministrador(self):
        f = _js_funcion("_pintarFinSaldoDom")
        self.assertIn("var _esSA = !!window.OT2C_ES_SUPERADMIN;", f)
        self.assertIn("if (_esSA) ['servicio','despacho'].forEach", f)
        self.assertIn("(_esSA && (d.usos||[]).length ?", f)

    def test_la_cuenta_con_rotulos_claros(self):
        f = _js_funcion("_pintarFinMargenDom")
        for t in ("Le cobras al cliente", "Saldo de sus facturas en Random", "Le pagas al proveedor (lo que te cobra)",
                  "Margen ($)", "Margen (%)"):
            self.assertIn(t, f, t)

    def test_sin_dialogos_nativos(self):
        for nombre in ("_pintarFinSaldoTxtDom", "_o2mFinAjuste", "_o2mFinPideMotivo"):
            self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", _js_funcion(nombre)), nombre)


if __name__ == "__main__":
    unittest.main()

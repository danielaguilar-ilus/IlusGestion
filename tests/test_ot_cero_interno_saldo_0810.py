"""Integracion fin-paso3 + main (08-oct-2026). Sin BD ni Flask (texto/ast):
  A. Un $0 escrito se respeta: se conserva, se envia como 0 y no hay respaldo (cotizacion/estimado) si la persona toco el
     valor; un cobro de $0 en una OT que se cobra sigue el camino de la rama (cobro_cero + autorizacion de Daniel).
  B. OT interna: el valor es SUGERIDO; vacio o $0 nunca traba crear ni firmar.
  C. Carrera del saldo: el chequeo toma un candado por documento (GET_LOCK) hasta el final de la peticion.
Correr con:  py -m unittest tests.test_ot_cero_interno_saldo_0810
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


M = _leer("templates/ot2/_modal_crear.html")
A = _leer("app.py")


def _funcion_js(src, nombre):
    i = src.index("function " + nombre + "(")
    j = src.index("\n  }\n", i)
    return src[i:j]


class TestCeroEscrito(unittest.TestCase):
    def test_limpiar_numero_conserva_el_cero(self):
        self.assertIn("String(v == null ? '' : v).replace(/[^0-9]/g,'')", _funcion_js(M, "_o2mLimpiarNumero"))

    def test_respaldos_solo_si_no_toco_el_valor(self):
        f = _funcion_js(M, "_o2mFinMontoServicio")
        self.assertIn("if (S.fin_zz_monto != null)", f)
        self.assertIn("!S.fin_valor_tocado && S.fin_cotizacion", f)
        self.assertIn("!S.fin_valor_tocado && S.fin_costo_estimado", f)

    def test_crear_usa_la_misma_fuente_y_sin_respaldos_sueltos(self):
        self.assertIn("var _finMs = _o2mFinMontoServicio();", M)
        self.assertNotIn("} else if (!S.fin_valor_tocado && S.fin_cotizacion", M)

    def test_vista_previa_manda_el_cero(self):
        f = _funcion_js(M, "_o2mFinVistaPrevia")
        self.assertIn("_o2mFinMontoServicio()", f)
        self.assertIn("zz_monto: (!gar && ms) ? ms.zz_monto : null", f)

    def test_cobro_cero_sin_garantia_sigue_el_camino_de_la_rama(self):
        # la pantalla: monto 0 en una OT que se cobra no avanza como monto; se declara garantia/regalia/arriendo
        self.assertIn("if (!S.fin_garantia && S.fin_zz_monto === 0) return false;", M)
        self.assertIn("if (S.fin_zz_monto === 0) return {cls:'falta'", M)
        # el servidor: 0 sin cobro_cero ni autorizacion -> FINANZAS_CERO_SIN_DECLARAR; con cobro_cero no pide el motivo de main
        self.assertIn('"FINANZAS_CERO_SIN_DECLARAR"', A)
        self.assertIn("not _fin_gar and not _fin_cero and _fin_zzm == 0 and not _fin_zz_motivo_manual", A)
        self.assertIn("if not _fin_cero and not _fin_aut_id and (_fin_zzm is None or _fin_zzm < 0):", A)

    def test_costo_interno_cero_viaja_como_cero(self):
        self.assertIn("esInterno() && String(S.costo_interno == null ? '' : S.costo_interno).trim() !== ''", M)


class TestInternaSugerida(unittest.TestCase):
    def test_pantalla_interna_pasa_siempre(self):
        self.assertIn("if (esInterno()) return true;", M)
        self.assertNotIn("if (esInterno()) return _o2mCostoInternoNum() > 0;", M)

    def test_subpaso_valor_interno_es_opcional(self):
        self.assertIn("{cls:'opcional', txt:'Opcional'};", _funcion_js(M, "_o2mFinSubEstado"))
        self.assertNotIn("_o2mCostoInternoNum() > 0 ? {cls:'listo'", M)

    def test_servidor_no_exige_valor_interno(self):
        self.assertNotIn('"FINANZAS_SIN_VALOR_INTERNO"', A)
        i = A.index("def _ot2_finanzas_estado(v):")
        j = A.index("# OJO: mant_visitas NO tiene una columna `garantia_aplica`", i)
        bloque = A[i:j]
        self.assertNotIn("faltan.append(\"valor", bloque)
        self.assertIn("return (not faltan), faltan", bloque)


class TestCarreraSaldo(unittest.TestCase):
    def test_chequear_toma_el_candado_antes_de_leer(self):
        i = A.index("def _ot_saldo_chequear(")
        j = A.index("def _ot_saldo_error_json", i)
        cuerpo = A[i:j]
        self.assertIn("_ot_saldo_reservar(docs)", cuerpo)
        self.assertLess(cuerpo.index("_ot_saldo_reservar(docs)"), cuerpo.index("_ot_saldo_de_docs("))

    def test_candado_por_documento_en_orden_y_no_bloquea(self):
        i = A.index("def _ot_saldo_reservar(docs):")
        j = A.index("def _ot_saldo_chequear(", i)
        c = A[i:j]
        self.assertIn("sorted(", c)           # orden fijo: sin interbloqueo
        self.assertIn("GET_LOCK(%s, 5)", c)   # espera acotada
        self.assertIn("except Exception", c)  # fail-open: nunca traba el trabajo

    def test_se_suelta_al_final_de_la_peticion(self):
        i = A.index("def close_db(exc=None):")
        c = A[i:i + 900]
        self.assertIn('g.pop("_ot_saldo_lock_conn", None)', c)
        self.assertIn(".close()", c)


if __name__ == "__main__":
    unittest.main()

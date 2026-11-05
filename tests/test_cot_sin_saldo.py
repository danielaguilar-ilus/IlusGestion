# -*- coding: utf-8 -*-
"""🔓 6-nov-2026 — Documentos SIN SALDO en el buscador de productos: desbloquear uno o varios con un comentario que queda en la actividad de
la cotización, y ordenar por fecha / filtrar por tipo de documento (GDV…).
Correr:  python -m pytest tests/test_cot_sin_saldo.py -q -p no:warnings"""
import os
import re
import shutil
import subprocess
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import cot_sin_saldo as ss  # noqa: E402


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


MODAL = _leer("templates", "tickets", "_tka_modal.html")
JS = _leer("static", "tickets_cotizaciones.js")
PY = _leer("tickets_module.py")


def _item(sku, tido="GDV", nudo="34190", motivo="El cliente pidió cotizar de nuevo", sin=True, nombre=None):
    return {"sku": sku, "nombre": nombre or ("Producto " + sku), "tido": tido, "nudo": nudo,
            "marcada_sin_saldo": sin, "motivo_sin_saldo": motivo}


class TestEventos(unittest.TestCase):
    def test_un_evento_por_comentario_con_sus_documentos(self):
        ev = ss.eventos_sin_saldo([_item("A"), _item("B"), _item("C", "VD", "10948"), _item("D", motivo="Otro motivo largo")])
        self.assertEqual(len(ev), 2)
        self.assertEqual(ev[0]["comentario"], "El cliente pidió cotizar de nuevo")
        self.assertEqual(ev[0]["documentos"], ["GDV 34190", "VD 10948"])
        self.assertEqual(ev[0]["total"], 3)
        self.assertEqual(ev[1]["total"], 1)

    def test_lo_que_tiene_saldo_o_no_trae_comentario_no_deja_rastro(self):
        self.assertEqual(ss.eventos_sin_saldo([_item("A", sin=False)]), [])
        self.assertEqual(ss.eventos_sin_saldo([_item("A", motivo="   ")]), [])
        self.assertEqual(ss.eventos_sin_saldo([{"sku": "X"}, "texto", None]), [])
        self.assertEqual(ss.eventos_sin_saldo(None), [])

    def test_no_repite_la_misma_linea_y_acota_el_tamano(self):
        ev = ss.eventos_sin_saldo([_item("A"), _item("a")])
        self.assertEqual(ev[0]["total"], 1)
        muchos = [_item("S%03d" % i) for i in range(200)]
        e = ss.eventos_sin_saldo(muchos)[0]
        self.assertEqual(e["total"], 200)
        self.assertEqual(len(e["productos"]), ss.MAX_PRODUCTOS_POR_EVENTO)
        largo = ss.eventos_sin_saldo([_item("A", motivo="x" * 900)])[0]["comentario"]
        self.assertEqual(len(largo), ss.MAX_COMENTARIO)


class TestModal(unittest.TestCase):
    def test_el_documento_sin_saldo_ya_no_esta_bloqueado(self):
        self.assertNotIn('title="Sin saldo: se eligen sus líneas una por una"', MODAL)
        self.assertIn('class="tka-ss-pick" onchange="tkaSSCambio()"', MODAL)

    def test_desbloqueo_masivo_pide_comentario_obligatorio_y_lo_pone_como_motivo(self):
        i = MODAL.index("async function tkaDesbloquearSinSaldo()")
        f = MODAL[i:i + 4200]
        self.assertIn("ilusPrompt", f)
        self.assertIn("required: true", f)
        self.assertIn("txt.length < 10", f)
        self.assertIn("sinSaldoMotivo: txt", f)
        self.assertNotRegex(f, r"\b(alert|confirm|prompt)\(")        # REGLA #1: nada de popups nativos

    def test_las_lineas_sin_saldo_entran_marcadas_con_su_motivo(self):
        i = MODAL.index("async function _tkaSelDocCompleto")
        f = MODAL[i:i + 2600]
        self.assertIn("marcada_sin_saldo: true, motivo_sin_saldo: sinSaldoMotivo", f)
        self.assertIn("if (!sinSaldoMotivo) return;", f)            # sin comentario, un documento sin saldo sigue sin entrar

    def test_seleccionar_todos_los_sin_saldo_es_un_toggle(self):          # REGLA #14
        i = MODAL.index("function tkaSSToggleTodos()")
        self.assertIn("cs.some(c => !c.checked)", MODAL[i:i + 400])
        self.assertIn("Seleccionar/deseleccionar todos los sin saldo", MODAL)

    def test_orden_y_filtro_reacomodan_sin_repintar(self):
        i = MODAL.index("function tkaCliOrdenar()")
        f = MODAL[i:i + 1700]
        self.assertIn("res.appendChild(card)", f)
        self.assertNotIn("innerHTML", f)                            # no se pierde lo expandido ni lo marcado
        for op in ("fecha_desc", "fecha_asc", "tipo_fecha"):
            self.assertIn('value="%s"' % op, MODAL)
        self.assertIn("Tipo de documento", MODAL)

    def test_una_linea_ya_desbloqueada_llega_marcada_al_reabrir_el_documento(self):
        self.assertIn("const preChecked = !!_selZ || (!isZero && !sinStockBod);", MODAL)

    def test_el_documento_se_sigue_pudiendo_elegir_linea_por_linea(self):    # REGLA #4.2
        self.assertIn("async function tkaToggleLineaCli(", MODAL)
        self.assertIn("_tkaPedirMotivoSinSaldo(nombre, autoReuse)", MODAL)

    def test_las_letras_nuevas_no_bajan_de_12px(self):
        i = MODAL.index("2026-11-06 (Daniel, viendo")
        bloque = MODAL[i:MODAL.index("async function tkaAsociarTodasConSaldo()")]
        for tam in re.findall(r"font-size:\s*([0-9.]+)rem", bloque):
            self.assertGreaterEqual(float(tam), 0.75, tam)

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_la_fecha_del_erp_se_lee_en_los_dos_formatos(self):
        m = re.search(r"function _tkaFechaTs\(s\)\{.*?\n\}\n", MODAL, re.S)
        self.assertTrue(m)
        codigo = m.group(0) + """
const a = _tkaFechaTs('27/10/2026'), b = _tkaFechaTs('2026-10-15'), c = _tkaFechaTs('11/05/2026 08:00'), d = _tkaFechaTs('');
console.log(JSON.stringify([a > b, b > c || c > b, d]));
console.log(a === Date.UTC(2026, 9, 27), b === Date.UTC(2026, 9, 15), c === Date.UTC(2026, 4, 11));"""
        r = subprocess.run(["node", "-e", codigo], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("true true true", r.stdout)
        self.assertIn(",0]", r.stdout)


class TestCotizacion(unittest.TestCase):
    def test_el_wizard_conserva_y_manda_el_comentario(self):
        self.assertEqual(JS.count("marcada_sin_saldo: !!it.marcada_sin_saldo, motivo_sin_saldo: it.motivo_sin_saldo || ''"), 2)

    def test_el_historial_sabe_mostrar_el_comentario_completo(self):
        self.assertIn("sin_saldo:'Documentos sin saldo desbloqueados'", JS)
        self.assertIn("accion === 'sin_saldo' && o.comentario", JS)

    def test_el_servidor_lo_registra_al_crear_y_al_editar(self):
        self.assertIn("import cot_sin_saldo as _cotsinsaldo", PY)
        self.assertEqual(PY.count("_tk_cotiz_log_sin_saldo(cot_id, items, user)"), 1)
        self.assertEqual(PY.count("_tk_cotiz_log_sin_saldo(cid, items, user)"), 1)
        i = PY.index("def _tk_cotiz_log_sin_saldo")
        self.assertIn('_tk_cotiz_log(cid, "sin_saldo", ev, user)', PY[i:i + 900])


if __name__ == "__main__":
    unittest.main()

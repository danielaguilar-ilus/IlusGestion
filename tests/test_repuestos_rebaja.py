# -*- coding: utf-8 -*-
"""🔒 6-nov-2026 — Ajustar la cantidad de un repuesto que ya existe (rebaja o aumento) = autorización del superadmin + argumento +
documento de Random OPCIONAL; el ajuste manual de cantidad queda cerrado (Daniel: «el ajuste manual es desorden y ya definitivamente no
se va a volver a realizar»). Este archivo cubre la lógica pura y las costuras del código; el comportamiento de las rutas con base falsa y la
pantalla viven en tests/test_repuestos_ajuste.py.
Correr:  python -m pytest tests/test_repuestos_rebaja.py -q -p no:warnings"""
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import repuestos_rebajas as rr  # noqa: E402


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


APP = _leer("app.py")
PANE = _leer("templates", "mantenciones", "_repuestos_bodega_pane.html")
HEAD = _leer("templates", "mantenciones", "repuestos_bodega.html")
HUB = _leer("templates", "clientes_hub", "repuestos.html")
ARG = "Se rompió en la instalación del cliente y quedó constatado por el técnico de turno."


class TestReglas(unittest.TestCase):
    def test_pedido_valido(self):
        self.assertIsNone(rr.validar_pedido(5, 2, ARG, "gdv", "34190"))

    def test_cantidad_debe_ser_positiva_y_no_pasar_el_saldo(self):
        self.assertIn("Indica cuántas", rr.validar_pedido(5, 0, ARG, "GDV", "1"))
        self.assertIn("Indica cuántas", rr.validar_pedido(5, "abc", ARG, "GDV", "1"))
        self.assertIn("No se puede rebajar", rr.validar_pedido(5, 6, ARG, "GDV", "1"))
        self.assertIsNone(rr.validar_pedido(5, 5, ARG, "GDV", "1"))

    def test_el_argumento_necesita_30_caracteres(self):
        self.assertIn("al menos 30", rr.validar_pedido(5, 1, "se rompió", "GDV", "1"))
        self.assertIn("al menos 30", rr.validar_pedido(5, 1, " " * 40, "GDV", "1"))

    def test_un_documento_a_medias_o_mal_formado_no_pasa(self):
        # El documento es opcional, pero si se escribe algo tiene que formar un documento válido.
        self.assertIn("documento", rr.validar_pedido(5, 1, ARG, "", "34190"))
        self.assertIn("documento", rr.validar_pedido(5, 1, ARG, "GDV", ""))
        self.assertIn("documento", rr.validar_pedido(5, 1, ARG, "G;DROP", "1"))

    def test_sin_documento_ya_no_es_un_error(self):
        self.assertIsNone(rr.validar_pedido(5, 1, ARG, "", ""))
        self.assertIsNone(rr.validar_pedido(5, 1, ARG, None, None))
        self.assertIsNone(rr.validar_pedido(5, 1, ARG, "  ", " "))

    def test_normalizar_y_etiqueta(self):
        self.assertEqual(rr.normalizar_doc(" gdv ", " 34190 "), ("GDV", "34190"))
        self.assertEqual(rr.normalizar_doc("X", "1"), ("", ""))
        self.assertEqual(rr.etiqueta_doc("GDV"), "Guía de despacho (GDV)")
        self.assertEqual(rr.etiqueta_doc("ZZZ"), "ZZZ")

    def test_rechazar_exige_motivo(self):
        self.assertIsNone(rr.validar_resolucion("aprobar", ""))
        self.assertIn("motivo", rr.validar_resolucion("rechazar", "no"))
        self.assertIsNone(rr.validar_resolucion("rechazar", "falta la guía"))
        self.assertEqual(rr.validar_resolucion("otra", "x"), "Acción no válida.")

    def test_la_nota_del_kardex_dice_quien_pidio_quien_autorizo_y_el_documento(self):
        n = rr.nota_kardex(7, "Lenin", "Daniel", "GDV", "34190", ARG)
        for t in ("#7", "Lenin", "Daniel", "GDV 34190", "Se rompió"):
            self.assertIn(t, n)
        self.assertLessEqual(len(n), rr.NOTA_KARDEX_MAX)
        auto = rr.nota_kardex(8, "Daniel", "Daniel", "FCV", "1", ARG, auto_autorizada=True)
        self.assertIn("(superadmin)", auto)
        largo = rr.nota_kardex(9, "A", "B", "GDV", "1", "x" * 900)
        self.assertEqual(len(largo), rr.NOTA_KARDEX_MAX)
        self.assertTrue(largo.endswith("…"))

    def test_al_aprobar_se_vuelve_a_mirar_el_saldo(self):
        self.assertEqual(rr.puede_aplicarse(5, 2), (True, ""))
        ok, msg = rr.puede_aplicarse(1, 2)
        self.assertFalse(ok)
        self.assertIn("saldo cambió", msg)


class TestServidor(unittest.TestCase):
    def test_el_ajuste_manual_de_cantidad_queda_cerrado(self):
        i = APP.index("def repstock_editar(rid):")
        f = APP[i:i + 9500]
        self.assertIn('"error_codigo": "AJUSTE_MANUAL_CERRADO"', f)
        self.assertIn("409", f)
        self.assertNotIn('objetivo=cantidad_nueva', f)               # ya no existe el camino que movía stock desde la edición
        self.assertNotIn('"ajuste", "ajuste_manual"', f)
        self.assertIn("Ajustar cantidad", f)                          # el mensaje manda al camino autorizado

    def test_reactivar_no_puede_ser_una_rebaja_disfrazada(self):
        i = APP.index("🔒 2026-11-06: reactivar solo puede SUBIR el saldo")
        self.assertIn("AJUSTE_MANUAL_CERRADO", APP[i:i + 900])

    def test_las_rutas_estan_protegidas(self):
        for ruta, nombre in (('"/mantenciones/api/repuestos-stock/<int:rid>/rebaja", methods=["POST"]', "repreb_pedir"),
                             ('"/mantenciones/api/repuestos-rebajas", methods=["GET"]', "repreb_listar"),
                             ('"/mantenciones/api/repuestos-rebajas/<int:bid>/resolver", methods=["POST"]', "repreb_resolver")):
            i = APP.index(ruta)
            cab = APP[i:i + 200]
            self.assertIn("@_mant_required", cab, nombre)
            self.assertIn("@_no_tecnico_externo", cab, nombre)         # el técnico externo no entra a la bodega
            self.assertIn("def " + nombre, cab, nombre)

    def test_solo_el_superadmin_autoriza_o_rechaza(self):
        i = APP.index("def repreb_resolver(bid):")
        f = APP[i:i + 6500]
        self.assertIn('if not g.permissions.get("superadmin"):', f)
        self.assertIn("Solo el superadmin autoriza o rechaza", f)
        self.assertIn("validar_resolucion", f)

    def test_el_pedido_valida_el_documento_en_random_solo_lectura(self):
        i = APP.index("def repreb_pedir(rid):")
        f = APP[i:i + 9500]
        self.assertIn("_erp_doc_lookup(tido, nudo)", f)               # REGLA #4.1: solo lectura
        self.assertIn("no existe en Random", f)
        self.assertIn("No se pudo consultar Random", f)                # «no responde» no es «no existe»
        self.assertIn("validar_pedido", f)
        self.assertIn('aplica_en_el_acto(g.permissions.get("superadmin"))', f)   # el superadmin se reconoce: se aplica en el acto
        self.assertIn("if con_doc:", f)                                # el documento es OPCIONAL: sin él no se consulta el ERP

    def test_aplicar_mueve_el_stock_por_el_motor_unico_con_constancia(self):
        i = APP.index("def _repreb_aplicar(")
        f = APP[i:i + 4800]
        self.assertIn("FOR UPDATE", f)
        self.assertIn("_repstock_mover(", f)
        self.assertIn("nota_kardex(", f)
        self.assertIn("puede_aplicarse(", f)
        self.assertIn("MOVIMIENTO[tipo]", f)                          # rebaja = salida · aumento = entrada
        self.assertEqual(rr.MOVIMIENTO["rebaja"], ("salida", "rebaja_autorizada"))
        self.assertEqual(rr.MOVIMIENTO["aumento"], ("entrada", "ajuste_autorizado"))

    def test_la_tabla_se_crea_barata_y_siempre(self):
        self.assertEqual(APP.count("CREATE TABLE IF NOT EXISTS mant_repuestos_rebajas ("), 1)
        i = APP.index("def _ensure_repuestos_rebajas():")
        f = APP[i:i + 3600]
        # REGLA #18: una sola sentencia ALTER, de UNA cláusula, por mysql_execute (que salta lo ya aplicado); nada más que bloquee.
        self.assertEqual(f.count("ALTER TABLE"), 1)
        self.assertIn("ALTER TABLE mant_repuestos_rebajas ADD COLUMN tipo VARCHAR(10) NOT NULL DEFAULT 'rebaja'", f)
        self.assertNotRegex(f, r"ADD COLUMN[^\"]*,\s*ADD")
        self.assertNotIn("cur.execute(", f.split("def _repreb_col_tipo_ok")[0])
        self.assertIn("_ensure_repuestos_rebajas()", APP[APP.index("🔒 2026-11-06: rebajas de repuestos con autorización (tabla barata"):][:400])

    def test_la_unica_escritura_de_stock_sigue_siendo_repstock_mover(self):
        self.assertEqual(len(re.findall(r"UPDATE mant_repuestos_stock SET cantidad=", APP)), 1)


class TestPantalla(unittest.TestCase):
    def test_la_cantidad_de_un_repuesto_existente_es_de_solo_lectura(self):
        self.assertIn("document.getElementById('rbCantidad').readOnly = true;", PANE)
        self.assertIn("document.getElementById('rbCantidad').readOnly = false;", PANE)
        self.assertIn("cantidad: id ? undefined : cantidad,", PANE)    # al editar la cantidad nunca viaja

    def test_hay_ajustar_cantidad_en_el_modal_y_en_el_menu_de_cada_fila(self):
        self.assertIn("rbAjusteDesdeModal()", PANE)
        self.assertIn("Ajustar cantidad", PANE)
        self.assertIn("rbAjusteAbrir({{ r.id }}", PANE)
        self.assertNotIn("Solicitar rebaja", PANE)

    def test_el_formulario_exige_argumento_y_el_documento_es_opcional(self):
        i = PANE.index("async function rbAjusteEnviar()")
        f = PANE[i:i + 3200]
        self.assertIn("arg.length < 30", f)
        self.assertIn("cant > RB_AJUSTE.saldo", f)
        self.assertIn("tido && !nudo", f)                               # solo falla si eligió un tipo sin número
        self.assertNotIn("!nudo){ ilusToast('Indica el número del documento de Random'", f)   # ya no se exige siempre
        self.assertNotRegex(f, r"\b(alert|confirm|prompt)\(")           # REGLA #1: nada de popups nativos

    def test_la_bandeja_autoriza_y_rechaza_con_los_helpers_de_ilus(self):
        i = PANE.index("async function rbAjusteResolver(")
        f = PANE[i:i + 3000]
        self.assertIn("ilusConfirm", f)
        self.assertIn("ilusPrompt", f)
        self.assertIn("required:true", f)
        self.assertNotRegex(f, r"\b(alert|confirm|prompt)\(")

    def test_boton_ajustes_junto_a_nuevo_repuesto_en_las_dos_pantallas(self):
        self.assertIn('id="rbAjustesBtn"', HEAD)
        self.assertIn("rbAjustesAbrir()", HEAD)
        self.assertIn("rbAbrirNuevo()", HEAD)                          # REGLA #4.2: nada se quitó
        self.assertIn('id="rbAjustesBtn"', HUB)                        # antes la bandeja no se alcanzaba desde la pestaña Bodega de /repuestos
        self.assertIn("rbAjustesAbrir()", HUB)
        self.assertIn("rbAbrirNuevo()", HUB)

    def test_la_pantalla_no_muestra_costos_ni_proveedores(self):       # REGLA #19/#26
        i = PANE.index("RB_AJUSTE_TIPOS_DOC")
        f = PANE[i:PANE.index("function rbCantidadCambio(){")]
        for prohibido in ("costo", "proveedor", "precio"):
            self.assertNotIn(prohibido, f.lower().replace("proveedores", ""), prohibido)


if __name__ == "__main__":
    unittest.main()

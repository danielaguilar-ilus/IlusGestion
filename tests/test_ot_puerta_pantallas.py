"""Las PANTALLAS de la puerta del documento — Daniel, 2026-10-07: "este módulo lo necesito potente, con espacios
ergonómicos donde se pueda visualizar todo el panorama… finalmente en el segundo modal, cuando firma el autorizador que
se va a cerrar la OT, necesito que ese mismo motor funcione".

Qué se vigila (sin BD ni Flask; textos de plantillas/JS con re y funciones de app.py con ast):
  · UN SOLO motor (static/ot_fin_motor.js + templates/ot2/_fin_motor.html) incluido en la ficha de la OT Y dentro del
    modal de aprobación y cierre, solo para gestión (REGLA #19), y el modal reacciona al rechazo de cierre con su acción.
  · El motor pinta el recorrido de 6 pasos, los contadores, TODOS los documentos con su detalle (nada desplegable),
    lo que nos costó y la cuenta; y escribe por los endpoints que ya registran quién y cuándo.
  · Sin alert()/confirm()/prompt() nativos en nada de lo nuevo (REGLA #1).
  · Asistente de creación: sin documento no se crea; «Pedir autorización a Daniel» (argumento ≥ 30) y «Autorizar y crear».
  · Páginas /ot/autorizaciones, /ot/autorizaciones/<id> y /ot/regularizar (+ sidebar), paginadas como Etiquetas.
  · Informe/PDF y Excel listan TODOS los documentos; al cliente solo tipo y número (nunca montos ni proveedores).
Correr con:  py -m unittest tests.test_ot_puerta_pantallas
"""
import ast
import os
import re
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


def _sin_comentarios_js(src):
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"(?m)^\s*//.*$", "", src)


NUEVOS_JS = ("static/ot_fin_motor.js", "static/ot_autorizaciones.js", "static/ot_regularizar.js")
NUEVAS_PLANTILLAS = ("templates/ot2/_fin_motor.html", "templates/ot2/autorizaciones.html",
                     "templates/ot2/autorizacion_detalle.html", "templates/ot2/regularizar.html")

_ARBOL = None


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = _codigo_y_arbol()
    return _ARBOL


def _cargar_helpers():
    _, arbol = _arbol()
    amb = {"print": lambda *a, **k: None, "re": re}
    quiero_f = ("_ot_doc_tipo_txt", "_ot_doc_categoria", "_ot_doc_real_a_usuario", "_ot_docs_para_informe",
                "_ot_docs_texto_lote")
    quiero_c = ("_OT_TIDO_TXT", "_OT_DOCS_COBRO", "_OT_DOCS_NOTA_VENTA")
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                and nodo.targets[0].id in quiero_c:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in quiero_f:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in quiero_f + quiero_c if n not in amb]
    assert not faltan, faltan
    return amb


class TestMotorUnico(unittest.TestCase):
    def test_archivos_del_motor_existen(self):
        for rel in NUEVOS_JS + NUEVAS_PLANTILLAS + ("static/ot_fin_motor.css", "static/ot_puerta_paginas.css"):
            self.assertTrue(os.path.isfile(os.path.join(RAIZ, rel.replace("/", os.sep))), rel)

    def test_motor_incluido_en_la_ficha_y_en_el_modal_de_cierre(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertEqual(det.count("ot2/_fin_motor.html"), 2, "el motor va en la ficha Y en el modal, ni más ni menos")
        self.assertIn("fm_modo='ficha'", det)
        self.assertIn("fm_modo='modal'", det)
        i_modal = det.index('id="otfModalAprobacion"')
        self.assertGreater(det.index("fm_modo='modal'"), i_modal, "el motor del cierre debe estar DENTRO del modal")
        # La ficha: a ancho completo (REGLA #16) y solo para gestión (REGLA #19).
        i_ficha = det.index("fm_modo='ficha'")
        self.assertIn('id="otdCardMotor"', det)
        self.assertIn("otd-span2", det[i_ficha - 400:i_ficha])
        self.assertIn("{% if not es_tecnico %}", det[i_ficha - 1500:i_ficha], "el motor de la ficha es solo para gestión")

    def test_el_modal_se_resuelve_ahi_mismo_cuando_el_cierre_rebota(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("OTFinMotor.alCerrarRechazado(d)", det)
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("alCerrarRechazado", js)
        for tipo in ("ligar_factura", "pedir_autorizacion", "esperar_autorizacion", "declarar_cobro",
                     "declarar_centro", "declarar_costo_proveedor", "actualizar_anexo"):
            self.assertIn(tipo, js, f"el rechazo «{tipo}» necesita su salida en el motor")

    def test_el_motor_pinta_todo_el_panorama(self):
        js = _leer("static/ot_fin_motor.js")
        for ruta in ("/recorrido", "/panorama", "/costo-proveedor", "/documentos", "/centro-costo", "/ot/api/autorizaciones"):
            self.assertIn(ruta, js)
        for texto in ("Facturas y boletas", "Notas de venta", "Cotizaciones", "Servicios", "Despachos", "Otros documentos",
                      "Todos los documentos de la OT", "Lo que nos costó", "La cuenta de esta OT", "Centro de costo",
                      "Esperando autorización de", "ya dada", "Dada de baja por la factura", "Fecha de emisión", "Lo ligó",
                      "El RUT coincide con el del cliente", "Pedir autorización a", "Corregir lo que cobró el proveedor",
                      "Autorizaciones de esta OT", "Servicio", "Despacho", "Productos"):
            self.assertIn(texto, js, texto)
        # Cada paso del recorrido con su acción cuando falta.
        for accion in ("ligarDoc", "pedirCero", "pedirCierre", "irFinanzas", "corregirProv", "enfocarCentro"):
            self.assertIn(accion, js)

    def test_nada_desplegable_ni_truncado(self):
        """REGLA #15: documentos y líneas SIEMPRE completos (sin <details>, acordeón, line-clamp ni elipsis)."""
        js = _leer("static/ot_fin_motor.js")
        css = _leer("static/ot_fin_motor.css")
        self.assertNotIn("<details", js)
        self.assertNotIn("collapse", js)
        self.assertNotIn("line-clamp", css)
        self.assertNotIn("text-overflow:ellipsis", css.replace(" ", ""))
        self.assertNotIn("-webkit-line-clamp", css)

    def test_movil_390_sin_scroll_horizontal_y_botones_tactiles(self):
        css = _leer("static/ot_fin_motor.css") + _leer("static/ot_puerta_paginas.css")
        self.assertIn("min-height:44px", css)
        self.assertIn("font-size:16px", css)
        self.assertIn("overflow-wrap:anywhere", css)
        self.assertRegex(css, r"@media \(max-width:(480|560)px\)")
        # La tarjeta del motor es de ancho completo en la grilla de la ficha (REGLA #16).
        self.assertIn(".otd-grid > #otdCardMotor{grid-column:1/-1}", _leer("static/ot_fin_motor.css"))

    def test_sin_dialogos_nativos(self):
        """REGLA #1: nada de alert()/confirm()/prompt() nativos en lo nuevo."""
        patron = re.compile(r"(?<![\w.$])(?:window\.)?(alert|confirm|prompt)\s*\(")
        for rel in NUEVOS_JS:
            src = _sin_comentarios_js(_leer(rel))
            self.assertIsNone(patron.search(src), f"{rel} usa un diálogo nativo")
        for rel in NUEVAS_PLANTILLAS:
            for m in re.finditer(r"<script(?![^>]*src)[^>]*>(.*?)</script>", _leer(rel), re.S):
                self.assertIsNone(patron.search(_sin_comentarios_js(m.group(1))), rel)
        modal = _leer("templates/ot2/_modal_crear.html")
        i = modal.index("function pedirAutorizacionCrear")
        self.assertIsNone(patron.search(modal[i:i + 3000]), "pedirAutorizacionCrear usa un diálogo nativo")

    def test_montos_solo_para_gestion_en_el_servidor(self):
        codigo, arbol = _arbol()
        fn = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "ot_api_panorama")
        texto = ast.get_source_segment(codigo, fn)
        self.assertIn("_es_rol_tecnico()", texto)
        self.assertIn("403", texto)
        self.assertNotIn("_ot_fin_rep_de(", texto)


class TestRutasYPaginas(unittest.TestCase):
    def test_rutas_registradas(self):
        codigo, _ = _arbol()
        for ruta in ('"/ot/api/<int:vid>/panorama"', '"/ot/api/<int:vid>/costo-proveedor"', '"/ot/autorizaciones"',
                     '"/ot/autorizaciones/<int:aid>"', '"/ot/regularizar"'):
            self.assertEqual(codigo.count("@app.route(" + ruta), 1, ruta)

    def test_costo_proveedor_pide_motivo_y_deja_registro(self):
        codigo, arbol = _arbol()
        fn = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "ot_api_costo_proveedor")
        t = ast.get_source_segment(codigo, fn)
        # 2026-10-08: la misma gestión, también con la OT 'completada' (ventana de «Firmar y cerrar»).
        self.assertIn("_ot_can_finanzas_cierre", [ast.get_source_segment(codigo, d) for d in fn.decorator_list])
        self.assertIn("MOTIVO_CORTO", t)
        self.assertIn("_mant_log(", t)
        self.assertIn("costo_proveedor_corregido", t)
        self.assertIn("OT_CERRADA", t)

    def test_autorizaciones_para_el_celular_de_daniel(self):
        js = _leer("static/ot_autorizaciones.js")
        for texto in ("ilusConfirm", "ilusPrompt", "required: true", "/aprobar", "/rechazar", "Argumento (completo)",
                      "La pidió", "Centro de costo", "Valorizado sugerido", "Qué se creará si apruebas",
                      "Mostrando", "Página", "Anterior", "Siguiente", "Por página"):
            self.assertIn(texto, js, texto)
        for rel in ("templates/ot2/autorizaciones.html", "templates/ot2/autorizacion_detalle.html"):
            self.assertIn("ot_autorizaciones.js", _leer(rel))

    def test_regularizar_paginada_con_filtros_y_semaforo(self):
        js = _leer("static/ot_regularizar.js")
        for texto in ("Mostrando", "Página", "Anterior", "Siguiente", "Por página", "/ot/api/regularizar", "cliente_q",
                      "creador", "mes", "estado", "falta", "ligarDocumento", "pedirAutorizacion", "pp-sem",
                      "Sin documento de Random", "falta la factura", "solo se regulariza"):
            self.assertIn(texto, js, texto)
        self.assertIn("ot_fin_motor.js", _leer("templates/ot2/regularizar.html"))
        codigo, _ = _arbol()
        self.assertIn('request.args.get("cliente_q")', codigo)

    def test_sidebar_con_los_dos_enlaces(self):
        base = _leer("templates/base.html")
        self.assertIn("ot_regularizar_pagina", base)
        self.assertIn("ot_aut_pagina", base)
        self.assertIn("sgAutBadge", base)
        i = base.index("ot_aut_pagina")
        self.assertIn("permissions.superadmin", base[i - 300:i])


class TestRegularizarCerradas(unittest.TestCase):
    """Una OT cerrada se regulariza ligando el documento; nunca se toca estado, firmas ni fechas."""

    def test_endpoint_solo_gestion_y_sin_tocar_el_estado(self):
        codigo, arbol = _arbol()
        fn = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "ot_api_documentos_regularizar")
        decos = [ast.get_source_segment(codigo, d) for d in fn.decorator_list]
        self.assertIn("_mant_required", decos)
        self.assertIn("_no_tecnico", decos)
        self.assertNotIn("_ot_can_cobertura", decos, "el candado de OT abierta lo revisa la función, no el decorador")
        t = ast.get_source_segment(codigo, fn)
        for texto in ("_ot_puede_regularizar()", "documento_regularizado", "estado, firmas y fechas intactos",
                      "NO_NECESITA", "__wrapped__"):
            self.assertIn(texto, t, texto)
        self.assertNotRegex(t, r"UPDATE\s+mant_visitas", "regularizar una OT cerrada no escribe en mant_visitas por su cuenta")
        self.assertIn("/documentos/regularizar", _leer("static/ot_fin_motor.js"))

    def test_quien_puede_regularizar(self):
        _, arbol = _arbol()
        amb = {"print": lambda *a, **k: None}
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name in ("_rol_familia", "_ot_puede_regularizar"):
                nodo.decorator_list = []
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        f = amb["_ot_puede_regularizar"]
        for rol in ("superadmin", "admin", "supervisor", "ejecutivo_sstt"):
            self.assertTrue(f({"role": rol}), rol)
        for rol in ("tecnico", "tecnico_externo", "tecnico_ejecutivo", ""):
            self.assertFalse(f({"role": rol}), rol)


class TestAsistenteDeCreacion(unittest.TestCase):
    def setUp(self):
        self.m = _leer("templates/ot2/_modal_crear.html")

    def test_sin_documento_se_pide_autorizacion_y_no_se_crea(self):
        for texto in ("pedirAutorizacionCrear", "'crear_sin_documento'", "/ot/api/autorizaciones", "/aprobar",
                      "Pidiendo autorización…", "Autorizando…", "Se pidió autorización a Daniel", "OT2C_ES_SUPERADMIN",
                      "Todavía no hay documento", "Pedir autorización a Daniel", "Autorizar y crear"):
            self.assertIn(texto, self.m, texto)
        i = self.m.index("function crear(){")
        antes_del_post = self.m[i:self.m.index("fetch('/ot/api/crear'", i)]
        self.assertIn("pedirAutorizacionCrear(body, _esSA, sig, _origSig)", antes_del_post,
                      "sin documento NO debe llegar a POST /ot/api/crear")

    def test_argumento_minimo_30_con_contador_y_tres_motivos(self):
        self.assertIn("length < 30", self.m)
        self.assertIn("de 30 caracteres como mínimo", self.m)
        for motivo in ("Garantía", "Regalía", "Arriendo o leasing"):
            self.assertIn(motivo, self.m)
        self.assertIn("body.finanzas.cobro_cero_motivo", self.m)
        self.assertIn("body.finanzas.cobro_cero_argumento", self.m)

    def test_superadmin_ve_autorizar_y_crear(self):
        self.assertIn("permissions.superadmin", self.m)
        self.assertIn("Eres superadministrador", self.m)

    def test_centro_de_costo_sigue_obligatorio(self):
        i = self.m.index("case 'finanzas':")
        self.assertIn("if (!S.fin_centro) return false;", self.m[i:i + 400])


class TestInformeYExcel(unittest.TestCase):
    def test_pdf_lista_todos_los_documentos_sin_montos(self):
        codigo, _ = _arbol()
        self.assertIn('ctx["documentos_ot"] = _ot_docs_para_informe(vid)', codigo)
        tpl = _leer("templates/mantenciones/ot_pdf.html")
        i = tpl.index('id="pdfDocumentosOt"')
        bloque = tpl[i:tpl.index("{% endif %}", i)]
        self.assertIn("d.tipo", bloque)
        self.assertIn("d.numero", bloque)
        for prohibido in ("monto", "costo", "proveedor", "zz_", "margen", "valoriz"):
            self.assertNotIn(prohibido, bloque.lower(), f"el informe al cliente no puede mostrar «{prohibido}»")

    def test_helper_del_informe_devuelve_solo_tipo_y_numero(self):
        amb = self._amb = _cargar_helpers()
        amb["_ot_docs_listar"] = lambda vid: {"documentos": [
            {"id": 1, "origen": "erp", "titulo": "FCV 11591", "monto": 1525000.0, "zz_serv": 252101.0, "rut": "76.1-9"},
            {"id": 2, "origen": "erp", "titulo": "VD 10666", "monto": 1525000.0},
            {"id": 3, "origen": "cotizacion", "titulo": "COT-000045", "monto": 99.0},
            {"id": 4, "origen": "erp", "titulo": "FCV 11591", "monto": 5.0}]}
        out = amb["_ot_docs_para_informe"](10)
        self.assertEqual(out, [{"tipo": "Factura", "numero": "11591"}, {"tipo": "Nota de venta", "numero": "10666"},
                               {"tipo": "Cotización", "numero": "COT-000045"}])
        for d in out:
            self.assertEqual(set(d), {"tipo", "numero"})

    def test_excel_lista_todos_los_documentos(self):
        codigo, _ = _arbol()
        self.assertIn("_docs_xl = _ot_docs_texto_lote(", codigo)
        self.assertIn("Documentos (todos los declarados)", codigo)
        amb = _cargar_helpers()
        amb["mysql_fetchall"] = lambda *a, **k: [
            {"visita_id": 7, "origen": "erp", "erp_tido": "NVV", "erp_nudo": "VD00010666", "reemplazado_por_id": 9,
             "numero_cotizacion": None},
            {"visita_id": 7, "origen": "erp", "erp_tido": "FCV", "erp_nudo": "11591", "reemplazado_por_id": None,
             "numero_cotizacion": None},
            {"visita_id": 8, "origen": "cotizacion", "erp_tido": None, "erp_nudo": None, "reemplazado_por_id": None,
             "numero_cotizacion": "COT-000045"}]
        out = amb["_ot_docs_texto_lote"]([7, 8, 9])
        self.assertIn("Nota de venta VD 10666 (dada de baja por factura)", out[7])
        self.assertIn("Factura FCV 11591", out[7])
        self.assertEqual(out[8], "Cotización COT-000045")
        self.assertEqual(out[9], "")

    def test_categorias_de_documento(self):
        amb = _cargar_helpers()
        self.assertEqual(amb["_ot_doc_categoria"]("FCV"), "cobro")
        self.assertEqual(amb["_ot_doc_categoria"]("BLV"), "cobro")
        self.assertEqual(amb["_ot_doc_categoria"]("VD"), "nota_venta")
        self.assertEqual(amb["_ot_doc_categoria"]("NVV"), "nota_venta")
        self.assertEqual(amb["_ot_doc_categoria"]("GDV"), "otro")
        self.assertEqual(amb["_ot_doc_tipo_txt"]("BLV"), "Boleta")


if __name__ == "__main__":
    unittest.main()

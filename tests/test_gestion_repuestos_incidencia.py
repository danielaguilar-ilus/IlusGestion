"""Tarjeta "Gestión de repuestos" del modal de Incidencias + empresa del técnico externo en el
Monitor de OT (Daniel, 2026-09-30).

Pedido (caso de estudio "Escaladora ILUS X1"): una tarjeta aparte donde se elige si el repuesto
existe o no en NUESTRA bodega, se adjunta evidencia (foto o video), y se genera una solicitud de
repuestos (varios a la vez, con proveedor) que viaja a Repuestos -> Solicitudes con ticket
(correlativo) y responsable. Y en el Monitor: "a los externos les figure la empresa".

Sin BD ni Flask: funciones puras de app.py extraídas con ast (mismo criterio que
tests/test_incidencias_bajas.py) más revisiones del código fuente y de las plantillas.

Correr con:  py -m unittest tests.test_gestion_repuestos_incidencia
"""
import ast
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_incidencias_bajas import _cargar, _decoradores, _plantilla  # noqa: E402
from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONITOR = os.path.join(RAIZ, "templates", "ot2", "monitor_tv.html")
TICKETS = os.path.join(RAIZ, "tickets_module.py")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


class TestTituloYNotaDelTicket(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_gr_titulo_ticket", "_inc_gr_nota_ticket", "_inc_gr_legacy"])
        cls.titulo = staticmethod(amb["_inc_gr_titulo_ticket"])
        cls.nota = staticmethod(amb["_inc_gr_nota_ticket"])
        cls.legacy = staticmethod(amb["_inc_gr_legacy"])

    def test_el_titulo_identifica_el_producto_y_la_incidencia(self):
        t = self.titulo(145, {"descripcion": "Escaladora ILUS X1", "sku": "1010089701"})
        self.assertEqual(t, "Repuestos · Escaladora ILUS X1 · SKU 1010089701 · incidencia #145")

    def test_el_titulo_sin_datos_sigue_siendo_valido_y_no_pasa_de_300(self):
        self.assertEqual(self.titulo(7, {}), "Repuestos · incidencia #7")
        self.assertLessEqual(len(self.titulo(7, {"descripcion": "x" * 500, "sku": "S"})), 300)

    def test_la_nota_lista_cada_linea_con_su_proveedor_y_marca_los_nuevos(self):
        lineas = [
            {"nombre": "Perno M8", "cantidad": 4.0, "repuesto_stock_id": 11, "prov_final": 1},
            {"nombre": "Pantalla táctil", "cantidad": 1.0, "repuesto_stock_id": None, "prov_final": None},
        ]
        txt = self.nota(145, {"descripcion": "Escaladora ILUS X1", "sku": "1010089701", "recomendacion": "UA1007933"},
                        lineas, [901, 902], {1: "Drax Fitness"}, "Falta el pedal izquierdo", "video")
        self.assertIn("lote de 2", txt)
        self.assertIn("#901 Perno M8 × 4 — Drax Fitness", txt)
        self.assertIn("#902 Pantalla táctil × 1 — sin proveedor · NO existe en la bodega (propuesto)", txt)
        self.assertIn("Producto: Escaladora ILUS X1 (SKU 1010089701) · UA UA1007933", txt)
        self.assertIn("Diagnóstico: Falta el pedal izquierdo", txt)
        self.assertTrue(txt.endswith("Evidencia: video."))

    def test_los_campos_de_la_tabla_se_llenan_solos(self):
        lineas = [{"nombre": "Perno M8", "cantidad": 4.0}, {"nombre": "Tuerca", "cantidad": 2.5}]
        self.assertEqual(self.legacy(lineas, True), ("si", "hay", "Perno M8 × 4, Tuerca × 2.5"))
        self.assertEqual(self.legacy(lineas, False)[1], "no_hay")

    def test_la_descripcion_no_pasa_de_300_caracteres(self):
        lineas = [{"nombre": "Repuesto con nombre bastante largo número %d" % i, "cantidad": 1.0} for i in range(30)]
        desc = self.legacy(lineas, False)[2]
        self.assertLessEqual(len(desc), 300)
        self.assertTrue(desc.endswith("…"))


class TestEmparejarExtras(unittest.TestCase):
    """El validador compartido fusiona los repuestos repetidos y no conoce el proveedor elegido a
    mano ni las casillas de "recordar": el emparejamiento tiene que caer en la línea correcta."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_gr_emparejar_extras", "_otrep_validar_lineas_lote"])
        cls.emparejar = staticmethod(amb["_inc_gr_emparejar_extras"])
        cls.validar = staticmethod(amb["_otrep_validar_lineas_lote"])

    def _stock(self):
        return {11: {"id": 11, "sku": "DR-0011", "descripcion": "Perno", "cantidad": 12, "proveedor_id": 1},
                12: {"id": 12, "sku": "DR-0012", "descripcion": "Tuerca", "cantidad": 0, "proveedor_id": None}}

    def test_proveedor_y_casillas_caen_en_su_linea(self):
        entrada = [
            {"origen": "bodega", "repuesto_stock_id": 11, "cantidad": 2, "proveedor_id": 1, "marcar_compatible": True},
            {"origen": "manual", "repuesto_nombre": "Pantalla nueva", "cantidad": 1, "proveedor_id": 3},
            {"origen": "bodega", "repuesto_stock_id": 12, "cantidad": 1, "proveedor_id": 2, "guardar_proveedor": True},
            {"origen": "manual", "repuesto_nombre": "Perno especial", "cantidad": 4},
        ]
        ok, err = self.validar(entrada, self._stock())
        self.assertIsNone(err)
        ex = self.emparejar(entrada, ok)
        self.assertEqual([e["proveedor_id"] for e in ex], [1, 3, 2, None])
        self.assertEqual([e["marcar_compatible"] for e in ex], [True, False, False, False])
        self.assertEqual([e["guardar_proveedor"] for e in ex], [False, False, True, False])

    def test_un_repuesto_repetido_se_fusiona_y_manda_el_primero(self):
        entrada = [
            {"origen": "bodega", "repuesto_stock_id": 11, "cantidad": 2, "proveedor_id": 1},
            {"origen": "manual", "repuesto_nombre": "Otro", "cantidad": 1, "proveedor_id": 3},
            {"origen": "bodega", "repuesto_stock_id": 11, "cantidad": 3, "proveedor_id": 2},
        ]
        ok, err = self.validar(entrada, self._stock())
        self.assertIsNone(err)
        self.assertEqual(len(ok), 2)
        self.assertEqual(ok[0]["cantidad"], 5)
        ex = self.emparejar(entrada, ok)
        self.assertEqual([e["proveedor_id"] for e in ex], [1, 3])

    def test_valores_raros_no_rompen(self):
        entrada = [{"origen": "manual", "repuesto_nombre": "X", "cantidad": 1, "proveedor_id": "abc",
                    "guardar_proveedor": "true", "marcar_compatible": 1}]
        ok, _ = self.validar(entrada, {})
        ex = self.emparejar(entrada, ok)
        self.assertEqual(ex, [{"proveedor_id": None, "guardar_proveedor": False, "marcar_compatible": False}])


class TestEndpointPlural(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("mant_api_incidencia_solicitar_repuestos")

    def test_mismos_candados_que_el_endpoint_singular(self):
        self.assertEqual(_decoradores("mant_api_incidencia_solicitar_repuestos"),
                         _decoradores("mant_api_incidencia_solicitar_repuesto"))
        self.assertIn("_mant_required", _decoradores("mant_api_incidencia_solicitar_repuestos"))
        self.assertIn("_no_tecnico_salvo_taller", _decoradores("mant_api_incidencia_solicitar_repuestos"))
        self.assertEqual(_decoradores("mant_api_incidencias_modelo_repuestos"),
                         ["app.route", "_mant_required", "_no_tecnico_salvo_taller"])

    def test_todo_se_valida_antes_de_escribir(self):
        pos = {k: self.src.find(k) for k in (
            "INC_GR_MOTIVO_MIN", "_otrep_validar_lineas_lote(", "_otrep_primer_archivo()", "_gcs_ready()",
            "INSERT INTO mant_ot_repuesto_solicitudes")}
        self.assertTrue(all(v > 0 for v in pos.values()), pos)
        self.assertLess(pos["INC_GR_MOTIVO_MIN"], pos["INSERT INTO mant_ot_repuesto_solicitudes"])
        self.assertLess(pos["_otrep_validar_lineas_lote("], pos["INSERT INTO mant_ot_repuesto_solicitudes"])
        self.assertLess(pos["_otrep_primer_archivo()"], pos["INSERT INTO mant_ot_repuesto_solicitudes"])
        self.assertLess(pos["_gcs_ready()"], pos["INSERT INTO mant_ot_repuesto_solicitudes"])

    def test_sin_evidencia_no_hay_solicitud_y_el_lote_entra_en_una_transaccion(self):
        self.assertIn("Sin evidencia no hay solicitud", self.src)
        self.assertIn("conn = get_db()", self.src)
        self.assertIn("conn.rollback()", self.src)
        # si la evidencia no queda registrada, el lote entero se descarta
        self.assertRegex(self.src, r"if not ok_ev:\s+for sid in sol_ids:\s+try:\s+mysql_execute\(\"DELETE FROM mant_ot_repuesto_solicitudes")

    def test_el_archivo_se_sube_una_vez_y_se_copia_a_las_demas_solicitudes(self):
        self.assertIn("info_out=info", self.src)
        self.assertIn("for sid in sol_ids[1:]:", self.src)

    def test_nace_como_solicitud_de_incidencia_con_lote_y_ticket(self):
        self.assertIn("'solicitado',%s,%s,'incidencia',%s", self.src)
        self.assertIn("_otrep_ticket_para_incidencia(iid, inc, user)", self.src)
        self.assertIn("lote_id", self.src)
        self.assertIn("asignado_a", self.src)   # responsable del ticket
        self.assertIn("tk_ticket_equipos", self.src)   # el producto en la ficha del ticket

    def test_lo_que_aprende_es_opcional_y_nunca_pisa(self):
        self.assertIn("proveedor_id IS NULL", self.src)      # solo si el repuesto no tenía proveedor
        self.assertIn("REPSTOCK_MAX_MODELOS", self.src)      # respeta el tope de modelos por repuesto
        self.assertIn("INSERT IGNORE INTO mant_repuestos_stock_modelos", self.src)

    def test_la_subida_generica_devuelve_la_info_solo_si_todo_salio_bien(self):
        src = _fuente_de("_otrep_subir_evidencia_generica")
        self.assertIn("info_out=None", src.split("\n")[0])
        self.assertIn("info_out.update", src)

    def test_el_titulo_del_ticket_de_la_incidencia_lleva_el_producto(self):
        self.assertIn("_inc_gr_titulo_ticket(iid, inc)", _fuente_de("_otrep_ticket_para_incidencia"))

    def test_la_ficha_trae_ticket_responsable_y_proveedor(self):
        src = _fuente_de("mant_api_incidencia_ficha")
        for campo in ("t.numero_ticket", "t.asignado_a AS responsable", "pv.nombre AS proveedor_nombre", "s.repuesto_stock_id"):
            self.assertIn(campo, src)


class TestColumnasSql(unittest.TestCase):
    """REGLA #5: verificar que las columnas nuevas de los SELECT/INSERT existen en su CREATE TABLE."""

    def test_columnas_de_las_solicitudes(self):
        ddl = _fuente_de("_ensure_ot_repuesto_solicitudes_tables")
        for col in ("incidencia_id", "repuesto_stock_id", "repuesto_nombre", "repuesto_sku", "origen", "cantidad",
                    "motivo", "estado", "solicitado_por", "proveedor_id", "creada_desde", "lote_id", "ticket_id",
                    "n_fotos", "n_videos"):
            self.assertRegex(ddl, r"\b" + col + r"\b", col)

    def test_columnas_del_producto_en_el_ticket(self):
        ddl = _leer(TICKETS).split("CREATE TABLE IF NOT EXISTS tk_ticket_equipos")[1].split("ENGINE=InnoDB")[0]
        for col in ("ticket_id", "nombre", "sku", "cantidad", "notas", "maquina_id"):
            self.assertRegex(ddl, r"\b" + col + r"\b", col)
        self.assertRegex(ddl, r"maquina_id\s+INT NULL")   # un producto sin máquina de cliente es válido

    def test_columnas_de_la_incidencia_que_se_sincronizan(self):
        src = _fuente_de("mant_api_incidencias_editar")
        for col in ("req_repuesto", "stock_repuesto", "descripcion_repuesto", "repuesto_stock_id", "updated_by"):
            self.assertIn(col, src)


class TestEmpresasDelMonitor(unittest.TestCase):
    def _correr(self, personas, filas_vinculo, filas_ficha, falla=False):
        llamadas = []

        def fake(sql, params=()):
            llamadas.append(sql)
            if falla:
                raise RuntimeError("BD caída")
            return filas_vinculo if "mant_tecnico_externo_usuarios" in sql else filas_ficha

        amb = _cargar(["_ot_tv_empresas_externas"], extra={"mysql_fetchall": fake})
        return amb["_ot_tv_empresas_externas"](personas), llamadas

    def test_la_empresa_sale_del_vinculo_de_la_ficha_de_la_empresa(self):
        personas = {5: {"externo": True, "proveedor_id": None}, 6: {"externo": False}}
        out, llamadas = self._correr(personas, [{"uid": 5, "empresa": " Transportes Felca "}], [])
        self.assertEqual(out, {5: "Transportes Felca"})
        self.assertEqual(len(llamadas), 1)   # nadie pendiente: no hay segunda consulta

    def test_si_no_esta_vinculado_se_usa_la_ficha_cruzada_por_nombre(self):
        personas = {5: {"externo": True, "proveedor_id": 9}, 7: {"externo": True, "proveedor_id": None}}
        out, _ = self._correr(personas, [], [{"id": 9, "razon_social": "DAP Servicio"}])
        self.assertEqual(out, {5: "DAP Servicio"})   # 7 no tiene empresa: sin etiqueta, sin inventar

    def test_los_internos_ni_se_consultan(self):
        out, llamadas = self._correr({1: {"externo": False}}, [], [])
        self.assertEqual((out, llamadas), ({}, []))

    def test_si_la_consulta_falla_el_monitor_sigue_sin_etiqueta(self):
        out, _ = self._correr({5: {"externo": True, "proveedor_id": 9}}, [], [], falla=True)
        self.assertEqual(out, {})

    def test_las_personas_del_monitor_llevan_la_empresa_solo_si_son_externas(self):
        src = _fuente_de("_ot_tv_datos")
        self.assertEqual(src.count('"empresa": _empresas.get(tid) if p["externo"] else None'), 2)
        self.assertGreaterEqual(src.count('"proveedor_id":'), 3)   # las tres formas de crear una persona

    def test_la_plantilla_del_monitor_pinta_la_empresa_en_la_fila_y_en_el_modal(self):
        html = _leer(MONITOR)
        self.assertIn('<span class="p-empresa"></span>', html)
        self.assertIn("setTexto(empEl, p.empresa || '')", html)
        self.assertIn("'Empresa: ' + p.empresa", html)


class TestPlantillaTarjeta5(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _plantilla()

    def test_las_seis_tarjetas_estan_numeradas_y_evidencia_es_la_6(self):
        pasos = re.findall(r'class="inc-num-circle" data-step="(\d)"', self.html)
        self.assertEqual(pasos, ["1", "2", "3", "4", "5", "6"])
        self.assertIn("INC_PASOS = 6", self.html)
        self.assertIn("<b>5. Gestión de repuestos</b>", self.html)
        self.assertIn("<b>6. Evidencia</b>", self.html)
        self.assertIn("0 de 6 pasos listos", self.html)

    def test_no_se_perdio_ningun_campo_de_repuestos_REGLA_4_2(self):
        for ident in ("incReqRepuesto", "incStockRepuesto", "incDescripcionRepuesto", "incRepuestoStockId",
                      "incBtnRepNoExiste", "incRepNuevoForm", "incRepNuevoNombre", "incRepNuevoProveedor",
                      "incBtnRepNuevoGuardar", "incRepSeleccionadoBox", "incBtnQuitarRepSel", "incBtnSolicitarAhora",
                      "incBtnSolicitarRepuesto", "incFechaResolucion", "incFechaIngreso", "incObservacion"):
            self.assertIn('id="' + ident + '"', self.html, ident)
        # el modal viejo de UN repuesto y su código siguen, solo que ya no lo abre ningún botón
        self.assertIn('id="incRepModal"', self.html)
        self.assertIn("function incAbrirModalRepuestoLegado()", self.html)
        self.assertIn("addEventListener('click', igrIrALaTarjeta)", self.html)

    def test_la_tarjeta_tiene_lo_que_pidio_daniel(self):
        for ident in ("igrProd", "igrDecide", 'data-modo="existe"', 'data-modo="noexiste"', 'data-modo="no"',
                      "igrBuscadorMount", "igrNxNombre", "igrNxProv", "igrLista", "igrEvCamBtn", "igrEvGalBtn",
                      "igrResp", "igrEnviar", "igrHist"):
            self.assertIn(ident, self.html, ident)
        self.assertIn("/mantenciones/api/incidencias/' + id + '/solicitar-repuestos", self.html)
        self.assertIn("/mantenciones/api/incidencias/modelo-repuestos?sku=", self.html)
        self.assertIn("/tickets/api/asignables", self.html)

    def test_el_envio_exige_repuestos_diagnostico_y_evidencia_y_guarda_la_incidencia_si_falta(self):
        bloque = self.html.split("function igrEnviar(){")[1].split("function igrPintarOk")[0]
        self.assertIn("Agrega al menos un repuesto", bloque)
        self.assertIn("motivo.length < 10", bloque)
        self.assertIn("Sin evidencia no hay solicitud", bloque)
        self.assertIn("incGuardarYLuego(function(){ igrEnviar(); })", bloque)
        self.assertIn("_igr.enviando", bloque)   # doble clic

    def test_el_detalle_ya_no_exige_repuestos_para_ponerse_en_verde(self):
        self.assertIn("4: {req: [], opc: []},", self.html)
        self.assertIn("5: {req: [], opc: ['igrGestion']},", self.html)
        self.assertIn("6: {req: [], opc: ['fotos']},", self.html)

    def test_el_buscador_se_usa_en_modo_agregar_con_el_modelo_y_en_masa(self):
        bloque = self.html.split("function igrAsegurarBuscador(){")[1].split("// ── La lista a pedir ──")[0]
        for clave in ("modo: 'agregar'", "modeloBase:", "masivo: true", "onAgregarTodos: igrAgregarTodos", "mostrarProveedor: true"):
            self.assertIn(clave, bloque)

    def test_todo_lo_que_viene_del_servidor_va_escapado(self):
        bloque = self.html.split("function igrPintarHist(){")[1].split("// La ficha (solicitudes")[0]
        for campo in ("s.repuesto_nombre", "s.proveedor_nombre", "tk.numero_ticket", "tk.responsable", "s.repuesto_sku"):
            self.assertRegex(bloque, r"igrEsc\(" + re.escape(campo))

    def test_el_buscador_compartido_sigue_siendo_compatible_hacia_atras(self):
        js = _leer(os.path.join(RAIZ, "static", "repuestos_buscador.js"))
        self.assertIn("compatDefault: opts.compatPorDefecto !== false", js)   # mismo valor por defecto de siempre
        self.assertIn("modeloBase: opts.modeloBase || null", js)
        self.assertIn("setModeloBase", js)
        self.assertIn("masivo: opts.masivo === true", js)
        # sin masivo el resumen sigue siendo texto plano, igual que antes
        self.assertIn("resumenBox.textContent = txtResumen", js)


if __name__ == "__main__":
    unittest.main()

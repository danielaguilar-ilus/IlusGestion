"""Tabla de verdad de «Facturas registradas» (Servicio Técnico). Daniel, 2026-10-08.

"Los lotes los ordenamos por número... acabo de hacer el de Felca y está dejando el de DAP Servicio primero
(tal vez por prioridad de pago)... y haz una TABLA DE VERDAD, como la de Retiros, que se pueda FILTRAR LAS COLUMNAS."

Lo que fijan estas pruebas:
  · El servidor entrega los lotes por NÚMERO de lote DESCENDENTE (el más nuevo primero), sin la prioridad
    FIELD(estado_pago) de antes; el Excel usa el mismo criterio. Nada de LIMIT/OFFSET por página en el SQL.
  · La plantilla es un <table> real con <thead>, encabezados ordenables, una fila de filtros por columna,
    pie de paginación (REGLA #4.3) y estilos rm-* de Retiros; conserva el tracking, los chips y los colores.
  · Las funciones puras de static/facprov_tabla.js (filtrar / ordenar / paginar / totales) se ejecutan con node.
  · En celular la tabla se vuelve tarjetas, con inputs de 16 px y botones de 44 px (REGLA #3).

Sin BD ni Flask: las funciones de app.py se extraen con ast.
Correr con:  py -m unittest tests.test_facturas_proveedor_tabla
"""
import ast
import datetime
import json
import os
import re
import shutil
import subprocess
import unittest

from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANTILLA = os.path.join(RAIZ, "templates", "mantenciones", "facturas_proveedor.html")
JS = os.path.join(RAIZ, "static", "facprov_tabla.js")
NODE = shutil.which("node")


def _leer(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def _amb():
    _, arbol = _codigo_y_arbol()
    amb = {"to_chile_filter": lambda v: v - datetime.timedelta(hours=3)}
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name in (
                "_facprov_ordenar_por_lote", "_facprov_fecha_fila", "_facprov_semaforo"):
            exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
    return amb


class TestOrdenEnElServidor(unittest.TestCase):
    def test_listado_por_numero_de_lote_descendente(self):
        src = _fuente_de("mant_facturas_proveedor")
        self.assertIn('" ORDER BY f.id DESC "', src)
        self.assertNotIn("FIELD(f.estado_pago", src, "ya no hay prioridad por estado de pago")
        self.assertNotIn("OFFSET", src, "la página la arma el navegador, no el SQL")

    def test_excel_usa_el_mismo_criterio(self):
        src = _fuente_de("mant_facturas_proveedor_xlsx")
        self.assertIn("GROUP BY f.id ORDER BY f.id DESC", src)
        self.assertNotIn("ORDER BY f.fecha ASC, f.id ASC", src)

    def test_ordenar_por_lote_ignora_proveedor_y_estado(self):
        a = _amb()
        lista = [
            {"id": 15, "estado_pago": "pendiente", "proveedor_nombre": "DAP Servicio"},
            {"id": 17, "estado_pago": "pagada", "proveedor_nombre": "Transportes felcarm SPA"},
            {"id": 16, "estado_pago": "pagada", "proveedor_nombre": "DAP Servicio"},
            {"id": 9, "estado_pago": "anulada", "proveedor_nombre": "Milling"},
        ]
        self.assertEqual([x["id"] for x in a["_facprov_ordenar_por_lote"](lista)], [17, 16, 15, 9])
        # el caso real: Felca (#17, pagada) NO queda debajo de DAP (#15, pendiente)
        self.assertEqual(a["_facprov_ordenar_por_lote"](lista)[0]["proveedor_nombre"], "Transportes felcarm SPA")

    def test_fecha_y_semaforo(self):
        a = _amb()
        f = a["_facprov_fecha_fila"]
        self.assertEqual(f({"fecha": datetime.date(2026, 10, 7)}), ("2026-10-07", "07/10/2026", "documento"))
        # sin documento: el día de registro, en hora Chile (aquí el doble resta 3 h)
        self.assertEqual(f({"created_at": datetime.datetime(2026, 10, 8, 2, 0)}), ("2026-10-07", "07/10/2026", "registro"))
        self.assertEqual(f({}), ("", "", ""))
        s = a["_facprov_semaforo"]
        self.assertEqual(s({"estado_pago": "anulada"}), "gris")
        self.assertEqual(s({"estado_pago": "pagada", "numero_oc": "OC-1", "numero_documento": "5"}), "verde")
        self.assertEqual(s({"estado_pago": "pagada", "numero_oc": "", "numero_documento": "5"}), "ambar")
        self.assertEqual(s({"estado_pago": "pendiente", "numero_oc": "OC-1", "numero_documento": "5", "n_ot": 2, "diferencia": 0}), "ambar")
        self.assertEqual(s({"estado_pago": "pendiente", "numero_oc": "OC-1", "numero_documento": "5", "n_ot": 2, "diferencia": -20000}), "rojo")


class TestPlantilla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _leer(PLANTILLA)
        # solo la sección de la tabla, para no confundirla con "OT por facturar"
        i = cls.html.index("{# ── Facturas registradas")
        j = cls.html.index("{# ── Modal: nueva factura")
        cls.tabla = cls.html[i:j]

    def test_jinja_valido(self):
        import jinja2
        env = jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
        env.parse(self.html)

    def test_es_una_tabla_real_con_estilos_de_retiros(self):
        t = self.tabla
        for pieza in ('<table class="rm-table', "<thead>", "</thead>", "<tbody>", 'id="fpvTabla"', "rm-toolbar",
                      "rm-search", "rm-wrap", "rm-foot", "rm-pill", 'data-alerta="'):
            self.assertIn(pieza, t, pieza)
        self.assertIn("retiros_monitor.css", self.html)
        self.assertIn("facprov_tabla.js", self.html)
        # sin copiar el CSS de Retiros: no hay una definición propia de la cabecera negra
        self.assertNotIn(".rm-table thead th{", self.html)

    def test_columnas_ordenables_y_filtro_por_columna(self):
        t = self.tabla
        for col in ("lote", "prov", "tracking", "oc", "fact", "fecha", "ot", "monto", "estado"):
            self.assertIn('data-sort="%s"' % col, t, col)
        for campo in ("lote", "prov", "etapa", "oc", "fact", "desde", "hasta", "ot", "montoMin", "montoMax", "estado", "q"):
            self.assertIn('data-fpt-f="%s"' % campo, t, campo)
        for texto in ("Limpiar filtros", "Solicitó", "OC emitida", "Factura recibida", "Pagada", "Falta el pago"):
            self.assertIn(texto, t, texto)
        self.assertIn('id="fpvCount"', t)  # «n de N facturas»

    def test_paginacion_como_etiquetas(self):
        t = self.tabla
        for pieza in ("fpvRango", "fpvSize", "fpvPrev", "fpvNext", "fpvPageInfo", "por página", "Anterior", "Siguiente"):
            self.assertIn(pieza, t, pieza)
        js = _leer(JS)
        self.assertIn("'Página '", js)
        self.assertIn("Mostrando <strong>", js)

    def test_no_se_pierde_informacion_de_la_fila(self):
        t = self.tabla
        for pieza in ("fp-trk-s", "fp-trk-c", "fp-trk-l", "Lote anulado", "fp-doc-oc", "Sin N° de OC", "Sin factura aún",
                      "con respaldo adjunto", "fp-otpill", "no cerrada", "sin anexo firmado", "Sin OT asignadas",
                      "Cuadra con las OT", "Pendiente de pago", "created_by", "prov_alias", "pagada_at", "pagada_por",
                      "registrada como", "/servicio-tecnico/facturas-proveedor/{{ f.id }}"):
            self.assertIn(pieza, t, pieza)

    def test_no_se_toca_el_resto_de_la_pantalla(self):
        # panel «Tu selección», OT por facturar, corte, solicitar OC, marcar pagada, nueva factura
        for pieza in ("fpvOtBar", "Tu selección", "fpvExcelCorte", "fpvAbrirExcel", "fpvModalNueva", "fpvCrear",
                      "fp-tabla fp-tabla-ot", "th_orden"):
            self.assertIn(pieza, self.html, pieza)

    def test_celular_tarjetas_y_toques(self):
        css = self.html[self.html.index("Tabla de verdad de \"Facturas registradas\""):]
        self.assertIn("@media (max-width:768px)", css)
        self.assertIn("min-height:44px;font-size:16px", css)  # inputs de filtro
        self.assertIn("tbody tr.rm-row{display:grid", css)  # la fila es una tarjeta
        self.assertIn("border-left:6px solid var(--rm-sem", css)  # semáforo en el borde
        self.assertIn("fpv-fl-open", css)

    def test_sin_alert_ni_confirm_nativos(self):
        self.assertNotRegex(_leer(JS), r"\b(alert|confirm|prompt)\s*\(")


@unittest.skipUnless(NODE, "node no está instalado")
class TestFuncionesPuras(unittest.TestCase):
    """Corre las funciones puras de facprov_tabla.js con node, con 12 facturas de varios proveedores."""

    @staticmethod
    def _correr(cuerpo):
        js = ("const F=require(%s);"
              "const D=[[17,'Transportes felcarm SPA','77.230.690-4','OC-9921','FACT 5521','2026-10-07','',6,3480000,'pagada',4],"
              "[16,'DAP Servicio','76.555.111-2','OC-9902','FACT 812','2026-10-05','',4,1085000,'pendiente',3],"
              "[15,'DAP Servicio','76.555.111-2','OC-9880','FACT 799','2026-10-01','',5,2250000,'pagada',4],"
              "[14,'LOGISTICA Y TRANSPORTES MILLING SPA','76.111.222-3','OC-9871','','','',3,1740000,'pendiente',2],"
              "[13,'LOGISTICA Y TRANSPORTES MILLING SPA','76.111.222-3','OC-9850','FACT 4410','2026-09-28','',2,960000,'pagada',4],"
              "[12,'Transportes felcarm SPA','77.230.690-4','','','','',0,0,'pendiente',1],"
              "[11,'DAP Servicio','76.555.111-2','OC-9801','FACT 770','2026-09-22','',3,1310000,'pendiente',3],"
              "[10,'Transportes felcarm SPA','77.230.690-4','OC-9790','FACT 5400','2026-09-20','',7,2800000,'pagada',4],"
              "[9,'LOGISTICA Y TRANSPORTES MILLING SPA','76.111.222-3','OC-9700','FACT 4380','2026-09-15','',1,500000,'anulada',0],"
              "[8,'DAP Servicio','76.555.111-2','','FACT 751','2026-09-12','',6,3100000,'pagada',4],"
              "[7,'Transportes felcarm SPA','77.230.690-4','OC-9650','FACT 5300','2026-09-05','',2,690000,'pendiente',3],"
              "[6,'LOGISTICA Y TRANSPORTES MILLING SPA','76.111.222-3','OC-9600','FACT 4300','2026-08-30','',5,2290365,'pagada',4]]"
              ".map(d=>F.prepararFila({id:d[0],nombre:d[1],prov:d[1]+' '+d[2],oc:d[3],fact:d[4],fecha:d[5],fechaLbl:d[5],ots:d[6],nOt:d[7],monto:d[8],estado:d[9],etapa:d[10]}));"
              "const ids=a=>a.map(x=>x.id);"
              "%s") % (json.dumps(JS.replace("\\", "/")), cuerpo)
        r = subprocess.run([NODE, "-e", js], capture_output=True, text=True, encoding="utf-8", timeout=60)
        if r.returncode != 0:
            raise AssertionError(r.stderr)
        return json.loads(r.stdout)

    def test_orden_por_defecto_lote_descendente(self):
        r = self._correr("process.stdout.write(JSON.stringify({ids:ids(F.ordenar(D.slice().reverse(),'lote','desc')),"
                         "asc:ids(F.ordenar(D,'lote','asc'))}));")
        self.assertEqual(r["ids"], [17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6])
        self.assertEqual(r["asc"], [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17])

    def test_ordenar_por_otras_columnas(self):
        r = self._correr(
            "process.stdout.write(JSON.stringify({"
            "monto:ids(F.ordenar(D,'monto','desc')).slice(0,3),"
            "prov:ids(F.ordenar(D,'prov','asc')).slice(0,4),"
            "estado:ids(F.ordenar(D,'estado','asc')),"
            "fecha:ids(F.ordenar(D,'fecha','desc')).slice(-2),"
            "oc:ids(F.ordenar(D,'oc','asc')).slice(-2)}));")
        self.assertEqual(r["monto"], [17, 8, 10])
        self.assertEqual(r["prov"], [16, 15, 11, 8])  # DAP Servicio primero, y dentro el lote más nuevo
        self.assertEqual(r["estado"], [16, 14, 12, 11, 7, 17, 15, 13, 10, 8, 6, 9])  # pendiente, pagada, anulada
        self.assertEqual(r["fecha"], [14, 12])  # sin fecha siempre al final, también al bajar
        self.assertEqual(r["oc"], [12, 8])  # sin OC siempre al final

    def test_filtros_por_columna(self):
        r = self._correr(
            "const q=o=>ids(F.ordenar(F.filtrar(D,o),'lote','desc'));"
            "process.stdout.write(JSON.stringify({"
            "lote:q({lote:'#15, 7'}),"
            "prov:q({prov:'felca'}),"
            "rut:q({prov:'761112223'}),"
            "oc:q({oc:'9902'}),"
            "fact:q({fact:'fact 54'}),"
            "estado:q({estado:'anulada'}),"
            "etapa_falta_oc:q({etapa:'f_oc'}),"
            "etapa_falta_fact:q({etapa:'f_fact'}),"
            "etapa_e3:q({etapa:'e3'}),"
            "etapa_pago:q({etapa:'f_pago'}),"
            "monto:q({montoMin:'1.000.000',montoMax:'2.300.000'}),"
            "fechas:q({desde:'2026-09-20',hasta:'2026-10-01'}),"
            "combo:q({prov:'dap',estado:'pagada'}),"
            "buscar:q({q:'Milling'}),"
            "vacio:q({lote:'',prov:'',oc:'',fact:'',ot:'',estado:'',etapa:'',montoMin:'',montoMax:'',desde:'',hasta:'',q:''})}));")
        self.assertEqual(r["lote"], [15, 7])
        self.assertEqual(r["prov"], [17, 12, 10, 7])
        self.assertEqual(r["rut"], [14, 13, 9, 6])  # por RUT, con o sin puntos
        self.assertEqual(r["oc"], [16])
        self.assertEqual(r["fact"], [10])
        self.assertEqual(r["estado"], [9])
        self.assertEqual(r["etapa_falta_oc"], [12, 8])
        self.assertEqual(r["etapa_falta_fact"], [14, 12])
        self.assertEqual(r["etapa_e3"], [16, 11, 7])
        self.assertEqual(r["etapa_pago"], [16, 14, 12, 11, 7])
        self.assertEqual(r["monto"], [16, 15, 14, 11, 6])
        self.assertEqual(r["fechas"], [15, 13, 11, 10])
        self.assertEqual(r["combo"], [15, 8])
        self.assertEqual(r["buscar"], [14, 13, 9, 6])
        # REGLA #4.3: con todos los filtros vacíos vuelven las 12
        self.assertEqual(r["vacio"], [17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6])

    def test_paginacion(self):
        r = self._correr(
            "process.stdout.write(JSON.stringify({a:F.paginar(12,1,10),b:F.paginar(12,2,10),c:F.paginar(12,9,10),"
            "d:F.paginar(0,1,25),e:F.paginar(12,1,100)}));")
        self.assertEqual((r["a"]["desde"], r["a"]["hasta"], r["a"]["paginas"]), (1, 10, 2))
        self.assertEqual((r["b"]["desde"], r["b"]["hasta"], r["b"]["pagina"]), (11, 12, 2))
        self.assertEqual(r["c"]["pagina"], 2)  # una página inexistente cae en la última
        self.assertEqual((r["d"]["desde"], r["d"]["hasta"], r["d"]["paginas"]), (0, 0, 1))
        self.assertEqual((r["e"]["desde"], r["e"]["hasta"], r["e"]["paginas"]), (1, 12, 1))

    def test_totales_y_formato(self):
        r = self._correr(
            "const t=F.totales(D,false);const f=F.totales(F.filtrar(D,{prov:'dap'}),false);"
            "process.stdout.write(JSON.stringify({t:t,f:f,anul:F.totales(F.filtrar(D,{estado:'anulada'}),true),"
            "clp:[F.clp(1085000),F.clp(0),F.clp(22205365),F.clp(-20000)],num:[F.numero('1.085.000'),F.numero(''),F.numero('abc')]}));")
        # las anuladas no suman (igual que el SQL del servidor)
        self.assertEqual((r["t"]["n"], r["t"]["monto"]), (11, 19705365))
        self.assertEqual(r["clp"], ["$1.085.000", "$0", "$22.205.365", "-$20.000"])
        self.assertEqual(r["num"], [1085000, None, None])
        self.assertEqual(r["anul"]["n"], 1)
        self.assertEqual(r["f"]["nPend"], 2)


if __name__ == "__main__":
    unittest.main()

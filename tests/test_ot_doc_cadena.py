"""TRAZABILIDAD EN RANDOM de los documentos de una OT (Daniel, 25-oct-2026).

«Es para agregar inteligencia a los cobros, ya que se les olvida que los servicios se facturan… sugieras el documento
en caso que lo tenga: FCV o BLV es lo que cierra una OT»; «si la ocupé en otra orden de trabajo, me debe arrojar el
saldo y el margen de esa OT… que no te metan gato por liebre… nació una nota de venta, se volvió factura o boleta y
después salió el stock con una guía de despacho».

Qué se vigila:
  · La lógica pura (ot_doc_cadena.py): agrupar por venta, la factura sugerida (caso real OT-2026-00277: VD 10414 →
    FCV 11745), nada sugerido si Random no respondió, alertas de «otra empresa» y de una factura que no viene de la
    nota de venta, margen de las otras OT, guías una sola vez y textos en pesos chilenos y dd/mm/aaaa.
  · El cableado en app.py: ruta con @_no_tecnico (REGLAS #19/#26), solo SELECT a Random (REGLA #4.1), nada de escritura.
  · El JS: sin alert/confirm/prompt nativos (REGLA #1), usa el diálogo de siempre (OTFinMotor.ligarDocumento) y escapa.
Sin BD, sin Flask, sin ERP.  Correr con:  python -m pytest -q tests/test_ot_doc_cadena.py
"""
import ast
import datetime as dt
import os
import re
import unittest

import ot_doc_cadena as C

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


NV = ("VD", "10414")
FCV = ("FCV", "11745")


def _saldo(serv_total, serv_saldo, desp_total=0, desp_saldo=0):
    return {"servicio": {"total": serv_total, "usado": serv_total - serv_saldo, "saldo": serv_saldo, "hay_lineas": bool(serv_total)},
            "despacho": {"total": desp_total, "usado": desp_total - desp_saldo, "saldo": desp_saldo, "hay_lineas": bool(desp_total)}}


def _ot277(familia_leida=True, mismo_cliente=True):
    docs = [{"clave": NV, "es_cobro": True, "reemplazado_por": None, "heredado": False}]
    fams = {NV: {"miembros": [NV, FCV], "leida": familia_leida}}
    info = {NV: {"fecha": "2026-10-06", "rut": "96831620-6", "razon_social": "Gestora Y Desarrollo Uss Spa",
                 "mismo_cliente": True, "saldo": _saldo(961640, 961640, 450000, 450000), "usos": []},
            FCV: {"fecha": dt.date(2026, 10, 6), "rut": "96831620-6" if mismo_cliente else "76111222-3",
                  "razon_social": "Gestora Y Desarrollo Uss Spa" if mismo_cliente else "Otra Empresa SpA",
                  "mismo_cliente": mismo_cliente, "saldo": _saldo(961640, 961640, 450000, 450000), "usos": []}}
    return docs, fams, info


class TestFormato(unittest.TestCase):
    def test_pesos_y_fechas_chilenas(self):
        self.assertEqual(C.clp(1411640), "$1.411.640")
        self.assertEqual(C.fecha_txt("2026-10-06 00:00:00"), "06/10/2026")
        self.assertEqual(C.fecha_txt(dt.date(2026, 10, 8)), "08/10/2026")
        self.assertEqual(C.fecha_txt(dt.datetime(2026, 10, 8, 15, 30)), "08/10/2026")
        self.assertEqual(C.fecha_txt(None), "")

    def test_tipos_en_palabras(self):
        self.assertEqual(C.tipo_txt(NV), "Nota de venta")
        self.assertEqual(C.tipo_txt(FCV), "Factura")
        self.assertEqual(C.tipo_txt(("BLV", "22719")), "Boleta")
        self.assertEqual(C.titulo(NV), "VD 10414")


class TestSugerencia(unittest.TestCase):
    def test_caso_real_ot277_sugiere_la_factura_de_la_nota_de_venta(self):
        docs, fams, info = _ot277()
        s = C.sugerencias(docs, fams, info)
        self.assertEqual(len(s), 1)
        self.assertEqual((s[0]["tipo"], s[0]["numero"]), ("FCV", "11745"))
        self.assertEqual(s[0]["nota"], "VD 10414")
        self.assertIn("ya se facturó en Random", s[0]["texto"])
        self.assertIn("06/10/2026", s[0]["texto"])
        self.assertIn("servicio $961.640", s[0]["texto"])
        self.assertIn("despacho $450.000", s[0]["texto"])
        self.assertEqual(s[0]["saldo_libre"], 1411640)
        self.assertNotIn("aviso_otra_empresa", s[0])
        self.assertIn("nota de venta VD 10414", s[0]["etiqueta"])

    def test_sin_lectura_de_random_no_se_sugiere_nada(self):
        docs, fams, info = _ot277(familia_leida=False)
        self.assertEqual(C.sugerencias(docs, fams, info), [])

    def test_no_se_sugiere_si_la_factura_ya_esta_en_la_ot(self):
        docs, fams, info = _ot277()
        docs[0]["reemplazado_por"] = FCV
        docs.append({"clave": FCV, "es_cobro": True, "reemplazado_por": None, "heredado": False})
        self.assertEqual(C.sugerencias(docs, fams, info), [])

    def test_no_se_sugiere_desde_una_copia_de_la_ot_madre(self):
        docs, fams, info = _ot277()
        docs[0]["heredado"] = True
        self.assertEqual(C.sugerencias(docs, fams, info), [])

    def test_factura_de_otra_empresa_se_sugiere_con_aviso(self):
        docs, fams, info = _ot277(mismo_cliente=False)
        s = C.sugerencias(docs, fams, info)
        self.assertEqual(len(s), 1)
        self.assertFalse(s[0]["mismo_cliente"])
        self.assertIn("otra empresa", s[0]["aviso_otra_empresa"])
        self.assertIn("76111222-3", s[0]["aviso_otra_empresa"])

    def test_saldo_consumido_se_avisa(self):
        docs, fams, info = _ot277()
        info[FCV]["saldo"] = _saldo(961640, 0, 450000, 0)
        s = C.sugerencias(docs, fams, info)
        self.assertTrue(s[0]["consumido"])
        self.assertIn("ya lo usan otras OT", s[0]["aviso_consumido"])


class TestAlertas(unittest.TestCase):
    def test_gato_por_liebre_la_factura_no_viene_de_la_nota(self):
        """La FCV 11800 dio de baja la VD 10414, pero Random dice que esa nota se facturó con la FCV 11745."""
        docs, fams, info = _ot277()
        otra = ("FCV", "11800")
        docs[0]["reemplazado_por"] = otra
        docs.append({"clave": otra, "es_cobro": True, "reemplazado_por": None, "heredado": False})
        fams[otra] = {"miembros": [otra], "leida": True}
        a = C.alertas(docs, fams, info)
        self.assertEqual([x["tipo"] for x in a], ["no_viene_de_la_nota"])
        self.assertIn("FCV 11800", a[0]["texto"])
        self.assertIn("FCV 11745", a[0]["texto"])

    def test_nota_sin_factura_en_random_dada_de_baja_igual(self):
        docs = [{"clave": NV, "reemplazado_por": ("FCV", "11800"), "heredado": False},
                {"clave": ("FCV", "11800"), "reemplazado_por": None, "heredado": False}]
        fams = {NV: {"miembros": [NV], "leida": True}}
        a = C.alertas(docs, fams, {})
        self.assertIn("todavía no tiene factura ni boleta", a[0]["texto"])

    def test_la_factura_correcta_no_alerta(self):
        docs, fams, info = _ot277()
        docs[0]["reemplazado_por"] = FCV
        docs.append({"clave": FCV, "reemplazado_por": None, "heredado": False})
        self.assertEqual(C.alertas(docs, fams, info), [])

    def test_sin_lectura_no_se_acusa_a_nadie(self):
        docs, fams, info = _ot277(familia_leida=False)
        docs[0]["reemplazado_por"] = ("FCV", "11800")
        self.assertEqual([x for x in C.alertas(docs, fams, info) if x["tipo"] == "no_viene_de_la_nota"], [])

    def test_documento_de_cobro_de_otra_empresa(self):
        docs, fams, info = _ot277(mismo_cliente=False)
        docs.append({"clave": FCV, "reemplazado_por": None, "heredado": False})
        a = C.alertas(docs, fams, info)
        self.assertEqual([x["tipo"] for x in a], ["otra_empresa"])
        self.assertIn("Otra Empresa SpA", a[0]["texto"])


class TestCadena(unittest.TestCase):
    def test_orden_nota_factura_guia_y_margen_de_otras_ot(self):
        docs, fams, info = _ot277()
        info[FCV]["usos"] = [{"vid": 249, "numero_ot": "OT-2026-00249", "cliente": "Physica Ppc", "url": "/ot/249",
                              "servicio": 252101, "despacho": 0}]
        info[FCV]["saldo"] = _saldo(961640, 709539, 450000, 450000)
        guias = {FCV: [{"numero": "0000004321", "fecha": "2026-10-08", "lineas": 2,
                        "productos": [{"sku": "BANCO1", "nombre": "Banco ajustable", "cantidad": 2}]}],
                 NV: [{"numero": "4321", "fecha": "2026-10-08", "lineas": 2, "productos": []}]}
        fins = {249: {"pct": 12.4, "queda": 31000, "mostrar": True}}
        r = C.armar(docs, fams, info, guias, fins)
        nodos = r["cadenas"][0]["nodos"]
        self.assertEqual([n["titulo"] for n in nodos], ["VD 10414", "FCV 11745", "GDV 4321"])  # la guía, una sola vez
        self.assertTrue(nodos[0]["en_ot"])
        self.assertFalse(nodos[1]["en_ot"])
        self.assertEqual(nodos[1]["estado_saldo"], "parcial")
        u = nodos[1]["usos"][0]
        self.assertEqual(u["margen_pct"], 12.4)
        self.assertEqual(u["nivel"], "verde")
        self.assertIn("tomó $252.101 de servicio", u["texto"])
        self.assertIn("margen 12 %", u["texto"])
        self.assertIn("08/10/2026", nodos[2]["texto"])
        self.assertEqual(len(r["sugerencias"]), 1)

    def test_margen_no_medible_no_se_inventa(self):
        u = {"vid": 1, "numero_ot": "OT-1", "servicio": 1000}
        self.assertIn("no se puede medir", C.texto_uso(u, {"mostrar": False, "pct": None}))
        self.assertIsNone(C.margen_nivel(None))
        self.assertEqual(C.margen_nivel(-3), "rojo")
        self.assertEqual(C.margen_nivel(4), "ambar")
        self.assertEqual(C.margen_nivel(25), "verde")

    def test_dos_ventas_distintas_son_dos_cadenas(self):
        otra_nv, otra_fcv = ("VD", "10500"), ("FCV", "11900")
        docs = [{"clave": NV, "reemplazado_por": None, "heredado": False},
                {"clave": otra_nv, "reemplazado_por": None, "heredado": False}]
        fams = {NV: {"miembros": [NV, FCV], "leida": True}, otra_nv: {"miembros": [otra_nv, otra_fcv], "leida": True}}
        r = C.armar(docs, fams, {}, {}, {})
        self.assertEqual(len(r["cadenas"]), 2)
        self.assertEqual(len(r["sugerencias"]), 2)

    def test_dada_de_baja_se_marca(self):
        docs, fams, info = _ot277()
        docs[0]["reemplazado_por"] = FCV
        docs.append({"clave": FCV, "reemplazado_por": None, "heredado": False})
        nodos = C.armar(docs, fams, info)["cadenas"][0]["nodos"]
        self.assertTrue(nodos[0]["dada_de_baja"])
        self.assertEqual(nodos[0]["reemplazado_por"], "FCV 11745")


class TestDocumentosAnteriores(unittest.TestCase):
    """OT-2026-00220 (27-oct): la BLV 23588 vivía solo en mant_visitas.factura_* y la tarjeta decía «no está en esta OT»
    y sugería agregarla, mientras el recorrido, el contador y el cierre la daban por buena."""

    @staticmethod
    def _clave(t, n):
        t, n = str(t).strip().upper(), str(n).strip().upper()
        return (t, n.lstrip("0") or "0")

    def _listado_220(self):
        # lo que devuelve _ot_docs_listar: la nota de venta de la tabla (con id) y la boleta solo en mant_visitas
        return [{"id": 41, "origen": "erp", "es_cobro": True, "es_principal": True, "titulo": "VD 10618"},
                {"id": None, "origen": "erp", "es_cobro": True, "es_principal": True, "titulo": "BLV 23588", "solo_legacy": True}]

    def test_el_principal_que_solo_vive_en_mant_visitas_es_un_documento_de_la_ot(self):
        existentes = [{"clave": ("VD", "10618"), "es_cobro": True, "reemplazado_por": None, "heredado": False}]
        extra = C.docs_anteriores(self._listado_220(), existentes, self._clave)
        self.assertEqual([tuple(d["clave"]) for d in extra], [("BLV", "23588")])
        self.assertTrue(extra[0]["es_cobro"])
        self.assertIsNone(extra[0]["reemplazado_por"])
        self.assertFalse(extra[0]["heredado"])

    def test_con_la_boleta_en_la_ot_ya_no_se_sugiere_agregarla(self):
        nv, blv = ("VD", "10618"), ("BLV", "23588")
        docs = [{"clave": nv, "es_cobro": True, "reemplazado_por": None, "heredado": False}]
        fams = {nv: {"miembros": [nv, blv], "leida": True}}
        info = {nv: {"saldo": _saldo(124440, 124440), "usos": []}, blv: {"saldo": _saldo(124440, 124440), "usos": []}}
        # antes (solo la tabla): falsa alarma
        self.assertEqual(len(C.sugerencias(docs, fams, info)), 1)
        # ahora: se suman los documentos anteriores a la tabla
        docs += C.docs_anteriores(self._listado_220(), docs, self._clave)
        fams[blv] = {"miembros": [nv, blv], "leida": True}
        self.assertEqual(C.sugerencias(docs, fams, info), [])
        cad = C.armar(docs, fams, info)
        self.assertEqual(cad["sugerencias"], [])
        nodos = {n["titulo"]: n for n in cad["cadenas"][0]["nodos"]}
        self.assertTrue(nodos["VD 10618"]["en_ot"])
        self.assertTrue(nodos["BLV 23588"]["en_ot"], "la boleta de la OT tiene que figurar «en esta OT»")

    def test_no_repite_lo_que_la_tabla_ya_trae_ni_toca_cotizaciones_o_filas_sin_tipo(self):
        existentes = [{"clave": ("BLV", "23588"), "es_cobro": True, "reemplazado_por": None, "heredado": False}]
        listado = self._listado_220() + [{"id": None, "origen": "cotizacion", "titulo": "COT-55"},
                                         {"id": None, "origen": "erp", "titulo": "FCV"},
                                         {"id": None, "origen": "erp", "titulo": ""},
                                         {"id": None, "origen": "erp", "es_cobro": False, "titulo": "FCV 00011439"},
                                         {"id": None, "origen": "erp", "titulo": "FCV 11439"}]
        extra = C.docs_anteriores(listado, existentes, self._clave)
        self.assertEqual([tuple(d["clave"]) for d in extra], [("FCV", "11439")], "una sola vez, con los ceros quitados")
        self.assertFalse(extra[0]["es_cobro"], "respeta si el documento anterior era solo de referencia")
        self.assertEqual(C.docs_anteriores(None, None, self._clave), [])

    def test_app_lee_los_documentos_anteriores_con_la_misma_funcion_que_el_panorama(self):
        app = _leer("app.py")
        ini = app.index("def _ot_doc_cadena(vid):")
        cuerpo = app[ini:app.index('@app.route("/ot/api/<int:vid>/documentos/cadena"')]
        self.assertIn("_cadena.docs_anteriores(_ot_docs_listar(vid)", cuerpo)


class TestCableado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _leer("app.py")
        ini = cls.app.index("_OT_CADENA_CACHE = {}")
        fin = cls.app.index('@app.route("/ot/api/<int:vid>/saldo-servicio/tomar"')
        cls.bloque = cls.app[ini:fin]

    def test_ruta_sin_tecnicos(self):
        m = re.search(r'@app\.route\("/ot/api/<int:vid>/documentos/cadena", methods=\["GET"\]\)\n((?:@\w+\n)+)def ', self.bloque)
        self.assertIsNotNone(m)
        self.assertIn("@_no_tecnico", m.group(1))
        self.assertIn("@_mant_required", m.group(1))
        self.assertIn("_es_rol_tecnico()", self.bloque)

    def test_random_solo_lectura_y_nada_se_escribe(self):
        self.assertNotIn("mysql_execute", self.bloque)
        self.assertNotIn("conn.commit", self.bloque)
        for sql in re.findall(r'_random_sql_query\(\s*((?:"[^"]*"\s*)+)', self.bloque):
            plano = "".join(re.findall(r'"([^"]*)"', sql)).strip().upper()
            self.assertTrue(plano.startswith("SELECT"), plano)
            for prohibido in ("INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", " INTO "):
                self.assertNotIn(prohibido, plano)

    def test_el_modulo_es_puro(self):
        arbol = ast.parse(_leer("ot_doc_cadena.py"))
        importados = {n.names[0].name for n in ast.walk(arbol) if isinstance(n, (ast.Import, ast.ImportFrom)) and n.names}
        mods = {getattr(n, "module", None) for n in ast.walk(arbol) if isinstance(n, ast.ImportFrom)}
        self.assertFalse({"flask", "pymysql", "pymssql", "app"} & (importados | mods))

    def test_js_amable_y_seguro(self):
        js = _leer("static/ot_doc_cadena.js")
        sin_coment = re.sub(r"/\*.*?\*/", "", js, flags=re.S)
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertIsNone(re.search(r"(?<![\w.])" + re.escape(nativo), sin_coment), nativo)
        self.assertIn("OTFinMotor.ligarDocumento", js)
        self.assertIn("function esc(", js)
        self.assertIn("/documentos/cadena", js)

    def test_se_carga_con_el_motor_y_el_dialogo_acepta_la_etiqueta(self):
        parcial = _leer("templates/ot2/_fin_motor.html")
        self.assertIn("ot_doc_cadena.js", parcial)
        self.assertIn("ot_doc_cadena.css", parcial)
        self.assertIn("valor: (pre && pre.etiqueta) || ''", _leer("static/ot_fin_motor.js"))


if __name__ == "__main__":
    unittest.main()

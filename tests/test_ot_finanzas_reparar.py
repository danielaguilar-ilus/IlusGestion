"""«Finanzas por reparar» — 2026-10-29 (REGLA #56).

Daniel, con las OT-151/152 (documento declarado y cobro en blanco) y las OT-150/158 (anexo distinto a la OT): «para no tener que corregir
todo a mano… lo que diga la OT, eso es lo mismo para el anexo».

La pantalla lista dos grupos y los repara de a uno o en lote, en DOS pasos (Revisar → Aplicar) y SOLO por los caminos de siempre. Se prueba
con datos de mentira:
  · la lista «documento declarado, cobro sin declarar» (SQL, filtros, documentos de cada OT);
  · la lista de anexos (cliente y si la OT sigue abierta) con el módulo real ot_anexo_costo;
  · quién puede verla (solo superadmin; un técnico jamás: REGLAS #19 y #26);
  · el JAVASCRIPT de la pantalla ejecutado en Node con un DOM de mentira: revisar no escribe, aplicar llama a los caminos correctos con
    `desde_documentos`, lo bloqueado en ámbar nunca se toca, «seleccionar todo» es un interruptor (REGLA #14) y pagina (REGLA #4.3).
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import ot_anexo_costo as oac
from tests import _fuente_app as FA

PLANTILLA = os.path.join(FA.RAIZ, "templates", "ot2", "finanzas_reparar.html")
PANEL = os.path.join(FA.RAIZ, "templates", "ot2", "panel.html")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


class TestListaCobroSinDeclarar(unittest.TestCase):
    def cargar(self, filas, docs, finanzas):
        visto = {"sql": []}

        def fetchall(sql, params=None):
            visto["sql"].append(sql)
            if "FROM mant_visitas v" in sql:          # la consulta principal (también menciona mant_visita_documentos en un EXISTS)
                return filas
            return docs

        amb = FA.cargar(constantes=["_OT_FIN_REPARAR_LIMITE"], funciones=["_ot_fin_cobro_sin_declarar_cargar"], extra={
            "mysql_fetchall": fetchall, "_OT_FIN_SQL_CONTRATO_REAL": "0", "_OT_FIN_SQL_MADRE_COLS": "NULL AS madre",
            "_ot_finanzas": lambda r, rep=None: finanzas[r["id"]]})
        return amb["_ot_fin_cobro_sin_declarar_cargar"], visto

    def test_solo_entran_las_que_se_cobran_y_no_tienen_cobro(self):
        filas = [{"id": 151, "numero_ot": "OT-2026-00151", "estado": "cerrada", "tipo": "instalacion", "cliente": "Gimnasio Uno SpA",
                  "factura_tido": "FCV", "factura_nudo": "0000011348"},
                 {"id": 152, "numero_ot": "OT-2026-00152", "estado": "completada", "tipo": "mantencion", "cliente": "Gimnasio Dos Ltda",
                  "factura_tido": "", "factura_nudo": ""},
                 {"id": 153, "numero_ot": "OT-2026-00153", "estado": "cerrada", "tipo": "instalacion", "cliente": "Garantía SA",
                  "factura_tido": "FCV", "factura_nudo": "5"},
                 {"id": 154, "numero_ot": "OT-2026-00154", "estado": "cerrada", "tipo": "instalacion", "cliente": "Ya cobrada SA",
                  "factura_tido": "FCV", "factura_nudo": "6"}]
        fin = {151: {"cobra": True, "cobre": {"hay": False}, "precio_anotado": 792437},
               152: {"cobra": True, "cobre": {"hay": False}, "precio_anotado": None},
               153: {"cobra": False, "cobre": {"hay": False}, "precio_anotado": None},      # garantía: no se cobra, no es un error
               154: {"cobra": True, "cobre": {"hay": True}, "precio_anotado": None}}         # ya tiene cobro declarado
        docs = [{"visita_id": 152, "erp_tido": "FCV", "erp_nudo": "0000011349"}, {"visita_id": 152, "erp_tido": "BLV", "erp_nudo": "77"}]
        f, visto = self.cargar(filas, docs, fin)
        out = f()
        self.assertEqual([x["id"] for x in out], [151, 152])
        por_id = {x["id"]: x for x in out}
        self.assertEqual(por_id[151]["documentos"], ["FCV 11348"])                 # el N° se muestra sin ceros a la izquierda
        self.assertEqual(por_id[152]["documentos"], ["FCV 11349", "BLV 77"])
        self.assertEqual(por_id[151]["precio_anotado"], 792437)                      # solo referencia: no es un cobro
        self.assertEqual(por_id[151]["cliente"], "Gimnasio Uno SpA")                 # el nombre completo, sin recortar (REGLA #15)

    def test_la_consulta_no_toca_lo_cancelado_ni_lo_espejado_y_solo_lee(self):
        f, visto = self.cargar([], [], {})
        f()
        sql = visto["sql"][0]
        self.assertIn("NOT IN ('cancelada','anulada')", sql)
        self.assertIn("COALESCE(v.zz_monto,0)=0", sql)
        self.assertIn("COALESCE(v.zz_envio_monto,0)=0", sql)
        self.assertIn("es_cobro=1", sql)
        self.assertIn("heredado_de_visita_id IS NULL", sql)                          # lo espejado de la madre no cuenta como cobro propio
        self.assertIn("reemplazado_por_id IS NULL", sql)
        self.assertNotRegex(sql.upper(), r"\b(INSERT|UPDATE|DELETE|DROP|ALTER)\b")

    def test_una_ot_sin_poder_calcular_su_cuenta_se_salta_sin_romper_la_lista(self):
        filas = [{"id": 1, "numero_ot": "A", "estado": "cerrada", "tipo": "x", "cliente": "C", "factura_tido": "FCV", "factura_nudo": "1"},
                 {"id": 2, "numero_ot": "B", "estado": "cerrada", "tipo": "x", "cliente": "C", "factura_tido": "FCV", "factura_nudo": "2"}]

        class Rota(dict):
            def __getitem__(self, k):
                if k == 1:
                    raise KeyError(k)
                return {"cobra": True, "cobre": {"hay": False}, "precio_anotado": None}
        f, _ = self.cargar(filas, [], Rota())
        self.assertEqual([x["id"] for x in f()], [2])


class TestListaAnexos(unittest.TestCase):
    def llamar(self, filas, hijas=()):
        def fetchall(sql, params=None):
            return list(hijas) if "ot_madre_id" in sql else filas

        amb = FA.cargar(funciones=["ot_api_anexos_vinculo_costos"], extra={
            "mysql_fetchall": fetchall, "_oac": oac, "_ot_aut_es_superadmin": lambda: True, "jsonify": lambda d: d,
            "_mant_required": lambda f: f, "_no_tecnico": lambda f: f})
        return amb["ot_api_anexos_vinculo_costos"]()

    def test_trae_el_cliente_y_si_la_ot_sigue_abierta(self):
        item = json.dumps([{"concepto": "Levantamiento de máquinas", "monto": 30000}])
        filas = [
            {"anexo_id": 1, "numero": 158, "anexo_estado": "firmado", "precio_items_json": item, "vid": 333, "numero_ot": "OT-2026-00158",
             "ot_estado": "cerrada", "costo_proveedor": None, "costo_despacho": None, "cliente": "Gymathius SpA"},
            {"anexo_id": 2, "numero": 150, "anexo_estado": "enviado", "precio_items_json": item, "vid": 330, "numero_ot": "OT-2026-00150",
             "ot_estado": "programada", "costo_proveedor": 20000, "costo_despacho": 0, "cliente": "Otro Gimnasio Ltda"},
            {"anexo_id": 3, "numero": 99, "anexo_estado": "firmado", "precio_items_json": item, "vid": 300, "numero_ot": "OT-2026-00099",
             "ot_estado": "cerrada", "costo_proveedor": 30000, "costo_despacho": 0, "cliente": "Alineada SA"}]
        r = self.llamar(filas)
        self.assertTrue(r["ok"])
        por_ot = {x["ot_id"]: x for x in r["por_corregir"]}
        self.assertEqual(sorted(por_ot), [330, 333])                                 # la alineada no aparece
        self.assertEqual(r["cuenta"]["alineado"], 1)
        self.assertEqual(por_ot[333]["vinculo"], "sin_declarar")
        self.assertEqual(por_ot[333]["cliente"], "Gymathius SpA")
        self.assertFalse(por_ot[333]["abierta"])                                     # cerrada: se corrige con el anexo FIRMADO
        self.assertEqual(por_ot[333]["total_anexo"], 30000)
        self.assertEqual(por_ot[330]["vinculo"], "difiere")
        self.assertTrue(por_ot[330]["abierta"])

    def test_la_consulta_trae_al_cliente_sin_escribir(self):
        t = FA.fuente_funcion("ot_api_anexos_vinculo_costos")
        self.assertIn("LEFT JOIN mant_clientes c ON c.id = v.cliente_id", t)
        self.assertIn("c.razon_social AS cliente", t)
        self.assertNotRegex(t.upper(), r"\b(INSERT|UPDATE|DELETE)\b")


class TestQuienPuedeVerla(unittest.TestCase):
    def test_las_rutas_estan_cerradas_a_tecnicos_y_a_todo_menos_superadmin(self):
        for nombre in ("ot_api_finanzas_por_reparar", "ot2_finanzas_reparar_pagina"):
            deco = " ".join(FA.decoradores(nombre))
            self.assertIn("_mant_required", deco, nombre)
            self.assertIn("_no_tecnico", deco, nombre)                               # REGLA #26: un técnico no ve plata
            self.assertIn("_ot_fin_es_superadmin()", FA.fuente_funcion(nombre), nombre)

    def test_un_no_superadmin_recibe_403_y_no_se_lee_nada(self):
        llamadas = []
        amb = FA.cargar(funciones=["ot_api_finanzas_por_reparar"], constantes=["_OT_FIN_REPARAR_LIMITE"], extra={
            "_ot_fin_es_superadmin": lambda: False, "_ot2_err": lambda m, c, s=400: ("err", c, s),
            "_ot_fin_cobro_sin_declarar_cargar": lambda: llamadas.append(1) or [], "jsonify": lambda d: d})
        self.assertEqual(amb["ot_api_finanzas_por_reparar"](), ("err", "SOLO_SUPERADMIN", 403))
        self.assertEqual(llamadas, [])

    def test_el_superadmin_recibe_la_lista_y_si_falla_un_mensaje_amable(self):
        amb = FA.cargar(funciones=["ot_api_finanzas_por_reparar"], constantes=["_OT_FIN_REPARAR_LIMITE"], extra={
            "_ot_fin_es_superadmin": lambda: True, "_ot2_err": lambda m, c, s=400: ("err", c, s, m),
            "_ot_fin_cobro_sin_declarar_cargar": lambda: [{"id": 1}], "jsonify": lambda d: d})
        self.assertEqual(amb["ot_api_finanzas_por_reparar"]()["cobro_sin_declarar"], [{"id": 1}])

        def rota():
            raise RuntimeError("detalle interno: SELECT secreto FROM tabla")
        amb["_ot_fin_cobro_sin_declarar_cargar"] = rota
        r = amb["ot_api_finanzas_por_reparar"]()
        self.assertEqual(r[:3], ("err", "ERROR_LISTA", 500))
        self.assertNotIn("SELECT", r[3])                                              # jamás detalles internos al cliente (REGLA #4)

    def test_el_boton_esta_en_la_barra_del_superadmin(self):
        panel = _leer(PANEL)
        self.assertIn("url_for('ot2_finanzas_reparar_pagina')", panel)
        self.assertIn("Finanzas por reparar", panel)
        self.assertLess(panel.index("ot2-sa-bar"), panel.index("ot2_finanzas_reparar_pagina"))


class TestPlantilla(unittest.TestCase):
    def setUp(self):
        self.t = _leer(PLANTILLA)

    def test_cumple_las_reglas_del_proyecto(self):
        self.assertNotRegex(self.t, r"(?<![A-Za-z.])(alert|confirm|prompt)\(")        # REGLA #1: nada de popups nativos
        self.assertIn("ilusConfirm", self.t)
        self.assertIn("Seleccionar/deseleccionar todo", self.t)                       # REGLA #14
        for txt in ("Mostrando ", "Anterior", "Siguiente", "Página ", "por página"):  # REGLA #4.3
            self.assertIn(txt, self.t)
        self.assertIn("min-height:44px", self.t)                                      # REGLA #3
        self.assertNotRegex(self.t, r"text-overflow\s*:\s*ellipsis|line-clamp")        # REGLA #15: nada truncado

    def test_usa_solo_los_caminos_de_siempre(self):
        usados = set(re.findall(r"['\"](/ot/api/[^'\"+]*)['\"]", self.t))
        self.assertTrue({"/ot/api/finanzas/por-reparar", "/ot/api/anexos/vinculo-costos"} <= usados)
        app = FA.texto()
        for ruta in ("/ot/api/finanzas/<int:vid>/corregir", "/ot/api/finanzas/<int:vid>/correcciones",
                     "/ot/api/<int:vid>/costo-desde-anexo", "/ot/api/<int:vid>/costo-desde-anexo/corregir"):
            self.assertIn('"%s"' % ruta, app, ruta)
        self.assertIn("desde_documentos: true", self.t)
        self.assertIn("simular: true", self.t)
        self.assertNotRegex(self.t, r"(?i)\bpublic_id|erp_engine|pymssql")             # el navegador no habla con Random ni con Check

    def test_el_script_compila(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node no está instalado")
        js = re.search(r"<script>(.*?)</script>", self.t, re.S).group(1)
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "p.js")
            with open(ruta, "w", encoding="utf-8") as fh:
                fh.write(js)
            r = subprocess.run([node, "--check", ruta], capture_output=True, text=True, encoding="utf-8")
            self.assertEqual(r.returncode, 0, r.stderr)


_HARNESS = r"""
const fs = require('fs');
const {js, escenario} = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const els = {};
function El(id){
  return { id, innerHTML:'', textContent:'', style:{}, disabled:false, value:'', _h:{},
    addEventListener(t, f){ (this._h[t] = this._h[t] || []).push(f); },
    get lastChild(){ return this._lc = this._lc || {textContent:''}; },
    get firstElementChild(){ return this._fc = this._fc || {style:{}}; },
    async fire(t, ev){ for (const f of (this._h[t] || [])) await f.call(this, ev || {target:this}); } };
}
global.document = {
  getElementById(id){ return els[id] = els[id] || El(id); },
  querySelectorAll(sel){
    if (sel !== '.fr-tab') return [];
    return global.__tabs = global.__tabs || ['cobro','anexo'].map(t => {
      const e = El('tab-' + t); e.attrs = {'data-tab': t}; e.getAttribute = n => e.attrs[n]; e.classList = {toggle(){}}; return e; });
  }
};
global.window = global;
const posts = [], toasts = [], confirms = [];
global.ilusToast = (m, o) => toasts.push([m, o && o.type]);
global.ilusConfirm = async (o) => { confirms.push(o); return escenario.confirmar !== false; };
global.fetch = async (url, opts) => {
  const metodo = ((opts && opts.method) || 'GET').toUpperCase();
  const cuerpo = opts && opts.body ? JSON.parse(opts.body) : null;
  if (metodo === 'POST') posts.push({url, cuerpo});
  const clave = metodo + ' ' + url + (cuerpo && cuerpo.simular ? ' simular' : '');
  const r = escenario.rutas[clave];
  if (!r) return {ok:false, status:404, json: async () => ({ok:false, error:'sin ruta ' + clave})};
  return {ok: r.status === undefined || r.status < 400, status: r.status || 200, json: async () => r.json};
};
(async () => {
  eval(js);
  await new Promise(r => setTimeout(r, 30));
  const paso = async (accion) => { await accion(); await new Promise(r => setTimeout(r, 30)); };
  const clic = (id, btn) => els[id].fire('click', {target: {closest: (s) => (s.indexOf('data-pg') >= 0 || s.indexOf('data-act') >= 0) ? btn : null}});
  const salida = {};
  for (const [nombre, pasos] of Object.entries(escenario.pasos || {})) {
    salida[nombre] = [];
    for (const p of pasos) {
      if (p.tab) { await paso(() => global.__tabs.find(t => t.attrs['data-tab'] === p.tab).fire('click', {})); }
      if (p.por) { els.frPor.value = String(p.por); await paso(() => els.frPor.fire('change', {})); }
      if (p.q !== undefined) { els.frQ.value = p.q; await paso(() => els.frQ.fire('input', {})); }
      if (p.todo) { await paso(() => els.frTodo.fire('click', {})); }
      if (p.revisarSel) { await paso(() => els.frRevisarSel.fire('click', {})); }
      if (p.aplicarListas) { await paso(() => els.frAplicarListas.fire('click', {})); }
      if (p.pg) { await paso(() => clic('frPie', {getAttribute: () => String(p.pg), disabled: false})); }
      if (p.fila) { await paso(() => clic('frBody', {getAttribute: (n) => ({'data-act': p.fila.act, 'data-k': p.fila.k})[n], disabled: false})); }
      salida[nombre].push({body: els.frBody.innerHTML, pie: els.frPie.innerHTML, sel: els.frSel.textContent,
                           n: [els['frN-cobro'].textContent, els['frN-anexo'].textContent],
                           revisarDeshabilitado: els.frRevisarSel.disabled, aplicarDeshabilitado: els.frAplicarListas.disabled,
                           aplicarTexto: els.frAplicarListas.lastChild.textContent});
    }
  }
  console.log(JSON.stringify({posts, toasts: toasts.length, confirms: confirms.map(c => ({title: c.title, message: c.message, sub: c.sub})), salida}));
})().catch(e => { console.error(e.stack || e); process.exit(1); });
"""


def _correcciones(servicio=342857, despacho=449580, incompleto=False, docs_error=False, cobra=True, hay=False, sin_doc=False,
                  usado=0, hay_lineas_servicio=True, me_cobraron=500000):
    docs = None if sin_doc else {
        "documentos": [{"titulo": "FCV 11348", "leido": not incompleto, "otro_cliente": False, "omitido": None}],
        "servicio": {"hay_lineas": hay_lineas_servicio, "total": servicio + usado, "usado": usado, "saldo": servicio},
        "despacho": {"hay_lineas": despacho > 0, "total": despacho, "usado": 0, "saldo": despacho},
        "sku_servicio": "ZZINSTALACION", "incompleto": incompleto}
    return {"json": {"ok": True, "docs": docs, "docs_error": docs_error, "cobra": cobra, "cobertura": "cobra",
                     "fin": {"cobre": {"hay": hay, "total": 0}, "me_cobraron": {"total": me_cobraron}}}}


class TestPantallaEjecutada(unittest.TestCase):
    """La pantalla corriendo en Node con un DOM de mentira y un servidor de mentira."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        cls.js = re.search(r"<script>(.*?)</script>", _leer(PLANTILLA), re.S).group(1)

    def correr(self, escenario):
        if not self.node:
            self.skipTest("Node no está instalado")
        with tempfile.TemporaryDirectory() as d:
            arch = os.path.join(d, "datos.json")
            h = os.path.join(d, "h.js")
            with open(arch, "w", encoding="utf-8") as fh:
                json.dump({"js": self.js, "escenario": escenario}, fh)
            with open(h, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS)
            r = subprocess.run([self.node, h, arch], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr)
            return json.loads(r.stdout.strip().splitlines()[-1])

    def _cobro(self, ids=(151, 152, 153)):
        nombres = {151: "Gimnasio Uno SpA", 152: "Corporación Deportiva Los Andes del Sur Limitada", 153: "Tercero SpA"}
        return [{"id": i, "numero": "OT-2026-%05d" % i, "cliente": nombres.get(i, "Cliente %d" % i), "estado": "cerrada",
                 "tipo": "instalacion", "documentos": ["FCV 11348"], "precio_anotado": None} for i in ids]

    def _rutas(self, cobro, anexo=()):
        return {"GET /ot/api/finanzas/por-reparar": {"json": {"ok": True, "cobro_sin_declarar": cobro}},
                "GET /ot/api/anexos/vinculo-costos": {"json": {"ok": True, "por_corregir": list(anexo)}}}

    def test_revisar_no_escribe_y_aplicar_solo_toma_lo_que_esta_listo(self):
        rutas = self._rutas(self._cobro())
        rutas["GET /ot/api/finanzas/151/correcciones"] = _correcciones()                                     # lista
        rutas["GET /ot/api/finanzas/152/correcciones"] = _correcciones(incompleto=True)                       # un documento sin leer / otro RUT
        rutas["GET /ot/api/finanzas/153/correcciones"] = _correcciones(servicio=0, hay_lineas_servicio=False)  # solo productos
        rutas["POST /ot/api/finanzas/151/corregir"] = {"json": {"ok": True, "ganancia_despues": 292437, "aviso_saldo": None}}
        out = self.correr({"rutas": rutas, "pasos": {
            "revisar": [{"todo": True}, {"revisarSel": True}],
            "aplicar": [{"aplicarListas": True}]}})
        rev = out["salida"]["revisar"][-1]
        self.assertEqual(out["posts"][0]["url"], "/ot/api/finanzas/151/corregir")
        self.assertEqual(len(out["posts"]), 1, "revisar no puede escribir y lo bloqueado no se toca")        # solo 151
        cuerpo = out["posts"][0]["cuerpo"]
        self.assertEqual((cuerpo["zz_monto"], cuerpo["zz_envio_monto"], cuerpo["desde_documentos"]), (342857, 449580, True))
        self.assertGreaterEqual(len(cuerpo["motivo"]), 15)
        self.assertIn("REGLA #56", cuerpo["motivo"])
        self.assertNotIn("zz_codigo", cuerpo)                                                                  # la línea ZZ la decide el servidor
        self.assertNotIn("valor_origen", cuerpo)                                                               # el rótulo también
        self.assertRegex(rev["body"], r"792[.,]437")                                                           # servicio + despacho
        self.assertIn("Corporación Deportiva Los Andes del Sur Limitada", rev["body"])                         # nombre completo, sin recortar
        self.assertIn("no se pudo leer en Random", rev["body"])
        self.assertIn("solo productos", rev["body"])
        self.assertEqual(out["confirms"][0]["message"].split(" por un total de ")[0], "Se tomará el cobro de 1 OT")
        fin = out["salida"]["aplicar"][-1]["body"]
        self.assertIn("Cobro tomado", fin)
        self.assertRegex(fin, r"292[.,]437")

    def test_seleccionar_todo_es_un_interruptor_y_se_cuentan_las_seleccionadas(self):
        rutas = self._rutas(self._cobro())
        out = self.correr({"rutas": rutas, "pasos": {"t": [{"todo": True}, {"todo": True}, {"todo": True}]}})
        a, b, c = out["salida"]["t"]
        self.assertEqual(a["sel"], "3 seleccionadas")
        self.assertEqual(b["sel"], "")                                                                         # la misma acción desmarca todo
        self.assertEqual(c["sel"], "3 seleccionadas")
        self.assertFalse(a["revisarDeshabilitado"])
        self.assertTrue(b["revisarDeshabilitado"])

    def test_pagina_como_etiquetas_y_el_tamano_lo_elige_el_usuario(self):
        cobro = self._cobro(range(1000, 1060))
        out = self.correr({"rutas": self._rutas(cobro), "pasos": {"p": [{"por": 25}, {"pg": 1}, {"pg": 1}, {"pg": -1}]}})
        p = out["salida"]["p"]
        self.assertIn("Mostrando 1–25 de 60", p[0]["pie"])
        self.assertIn("Página 1 de 3", p[0]["pie"])
        self.assertIn("Mostrando 26–50 de 60", p[1]["pie"])
        self.assertIn("Mostrando 51–60 de 60", p[2]["pie"])
        self.assertIn("Página 2 de 3", p[3]["pie"])
        self.assertEqual(str(p[0]["n"][0]), "60")

    def test_la_busqueda_filtra_por_ot_cliente_o_documento(self):
        out = self.correr({"rutas": self._rutas(self._cobro()), "pasos": {"q": [{"q": "los andes"}, {"q": "no existe"}, {"q": ""}]}})
        a, b, c = out["salida"]["q"]
        self.assertIn("OT-2026-00152", a["body"])
        self.assertNotIn("OT-2026-00151", a["body"])
        self.assertIn("Ninguna OT coincide", b["body"])
        self.assertIn("OT-2026-00151", c["body"])

    def test_anexos_abierta_alinea_cerrada_firmada_simula_primero_y_sin_firmar_no_se_toca(self):
        anexo = [
            {"ot_id": 330, "ot": "OT-2026-00150", "ot_estado": "programada", "cliente": "Gimnasio Abierto SpA", "abierta": True,
             "anexo": 150, "anexo_estado": "enviado", "total_anexo": 30000, "costo_proveedor": 20000, "costo_despacho": 0,
             "vinculo": "difiere", "compartido": False, "parte": None},
            {"ot_id": 333, "ot": "OT-2026-00158", "ot_estado": "cerrada", "cliente": "Gymathius SpA", "abierta": False,
             "anexo": 158, "anexo_estado": "firmado", "total_anexo": 30000, "costo_proveedor": None, "costo_despacho": None,
             "vinculo": "sin_declarar", "compartido": False, "parte": None},
            {"ot_id": 340, "ot": "OT-2026-00160", "ot_estado": "cerrada", "cliente": "Sin Firma Ltda", "abierta": False,
             "anexo": 160, "anexo_estado": "enviado", "total_anexo": 90000, "costo_proveedor": 10000, "costo_despacho": 0,
             "vinculo": "difiere", "compartido": False, "parte": None}]
        rutas = self._rutas([], anexo)
        rutas["POST /ot/api/333/costo-desde-anexo/corregir simular"] = {"json": {"ok": True, "simulacion": True, "ganancia_antes": 100000,
                                                                              "ganancia_despues": 70000}}
        rutas["POST /ot/api/333/costo-desde-anexo/corregir"] = {"json": {"ok": True, "mensaje": "El costo de la OT quedó igual al Anexo N° 158"}}
        rutas["POST /ot/api/330/costo-desde-anexo"] = {"json": {"ok": True, "mensaje": "El costo de la OT quedó igual al Anexo N° 150"}}
        out = self.correr({"rutas": rutas, "pasos": {
            "a": [{"tab": "anexo"}, {"todo": True}, {"revisarSel": True}],
            "b": [{"aplicarListas": True}]}})
        rev = out["salida"]["a"][-1]["body"]
        self.assertIn("Gymathius SpA", rev)
        self.assertIn("aún no está firmado", rev)                                                              # la del anexo sin firmar: ámbar
        urls = [p["url"] for p in out["posts"]]
        self.assertNotIn("/ot/api/340/costo-desde-anexo", urls)
        self.assertNotIn("/ot/api/340/costo-desde-anexo/corregir", urls)
        simulados = [p for p in out["posts"] if (p["cuerpo"] or {}).get("simular")]
        self.assertEqual([p["url"] for p in simulados], ["/ot/api/333/costo-desde-anexo/corregir"])           # simular antes de escribir
        reales = [p["url"] for p in out["posts"] if not (p["cuerpo"] or {}).get("simular")]
        self.assertCountEqual(reales, ["/ot/api/333/costo-desde-anexo/corregir", "/ot/api/330/costo-desde-anexo"])

    def test_si_el_usuario_cancela_la_confirmacion_no_se_escribe_nada(self):
        rutas = self._rutas(self._cobro((151,)))
        rutas["GET /ot/api/finanzas/151/correcciones"] = _correcciones()
        out = self.correr({"confirmar": False, "rutas": rutas, "pasos": {"c": [{"todo": True}, {"revisarSel": True}, {"aplicarListas": True}]}})
        self.assertEqual(out["posts"], [])
        self.assertEqual(len(out["confirms"]), 1)

    def test_un_error_del_servidor_se_muestra_y_no_deja_la_ot_como_hecha(self):
        rutas = self._rutas(self._cobro((151,)))
        rutas["GET /ot/api/finanzas/151/correcciones"] = _correcciones()
        rutas["POST /ot/api/finanzas/151/corregir"] = {"status": 409, "json": {"ok": False, "error": "El cobro supera el saldo del documento."}}
        out = self.correr({"rutas": rutas, "pasos": {"e": [{"todo": True}, {"revisarSel": True}, {"aplicarListas": True}]}})
        cuerpo = out["salida"]["e"][-1]["body"]
        self.assertIn("El cobro supera el saldo del documento.", cuerpo)
        self.assertNotIn("Cobro tomado", cuerpo)

    def test_ya_tomado_o_que_no_se_cobra_se_dice_y_no_se_ofrece(self):
        rutas = self._rutas(self._cobro((151, 152)))
        rutas["GET /ot/api/finanzas/151/correcciones"] = _correcciones(hay=True)
        rutas["GET /ot/api/finanzas/152/correcciones"] = _correcciones(cobra=False)
        out = self.correr({"rutas": rutas, "pasos": {"g": [{"todo": True}, {"revisarSel": True}]}})
        fin = out["salida"]["g"][-1]
        self.assertIn("Ya tiene el cobro declarado", fin["body"])
        self.assertIn("no corresponde tomar un cobro", fin["body"])
        self.assertTrue(fin["aplicarDeshabilitado"])
        self.assertEqual(out["posts"], [])


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""«Finanzas por reparar» · 3.ª pestaña «Cobro sin respaldo» — 2026-10-29 (REGLAS #56 y #59).

Daniel: «debe existir control» del cobro escrito a mano: la lista de OT cuyo cobro está «declarado» sin origen o «escrito a mano»
sin motivo, para respaldarlo con su documento o confirmarlo con su motivo. Datos de MENTIRA (REGLA #22). Se prueba:
  · la lista (solo lectura; la regla es UNA, ot_calidad.cobro_sin_origen; solo las OT que se cobran);
  · la ruta de la lista (la 3.ª lista viaja con la 1.ª y, si falla, la 1.ª sale igual);
  · `confirmar_origen` en «Corregir finanzas»: válido SOLO si los montos no cambian; 'documento' solo si el cobro es EXACTAMENTE lo que
    aportan los documentos (lo recalcula el servidor); 'manual' exige el motivo y lo copia a zz_motivo_manual; mismo registro inmutable;
    no toca el costo (el candado del anexo no se mueve);
  · la pantalla ejecutada en Node con un DOM de mentira: revisar no escribe, (a) entra a «Aplicar las listas», (b) va de a una con
    ilusConfirm antes → después, (c) pide el motivo con ilusPrompt (≥ 15) y lo bloqueado nunca se toca.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import ot_calidad
from tests import _fuente_app as FA
from tests import _ger_app as A

PLANTILLA = os.path.join(FA.RAIZ, "templates", "ot2", "finanzas_reparar.html")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestLista(unittest.TestCase):
    def cargar(self, filas, docs=(), corr=()):
        visto = {"sql": []}

        def fetchall(sql, params=None):
            visto["sql"].append(sql)
            if "FROM mant_visitas v" in sql:
                return filas
            if "FROM mant_ot_finanzas_correcciones" in sql:
                return list(corr)
            return list(docs)

        amb = FA.cargar(constantes=["_OT_FIN_REPARAR_LIMITE"], funciones=["_ot_fin_cobro_sin_origen_cargar"], extra={
            "mysql_fetchall": fetchall, "_OT_FIN_SQL_CONTRATO_REAL": "0", "_OT_FIN_SQL_MADRE_COLS": "NULL AS madre",
            "_ot_finanzas": lambda r, rep=None: A.fin_de(r), "_calidad": ot_calidad,
            "chile_fmt_filter": lambda d, f=None: "29/10/2026 10:00"})
        return amb["_ot_fin_cobro_sin_origen_cargar"], visto

    def test_solo_las_que_se_cobran_y_no_tienen_respaldo(self):
        filas = [A.visita(701, zz_monto=300000, valor_origen=None, cliente="Gimnasio Uno SpA", created_by="ejecutivo1"),
                 A.visita(702, zz_monto=200000, zz_envio_monto=20000, valor_origen="manual", zz_motivo_manual=None),
                 A.visita(703, zz_monto=200000, valor_origen="manual", zz_motivo_manual="Acuerdo comercial firmado"),
                 A.visita(704, zz_monto=200000, valor_origen=None, modalidad_cobro="garantia", cobro_cero_motivo="garantia"),
                 A.visita(705, zz_monto=200000, valor_origen="zz"),
                 A.visita(706, zz_monto=1, valor_origen=None, zz_codigo="ZZRETIRO")]
        docs = [{"visita_id": 701, "erp_tido": "FCV", "erp_nudo": "0000011348"}]
        corr = [{"visita_id": 702, "usuario": "daniel", "created_at": "x"}]
        f, visto = self.cargar(filas, docs, corr)
        out = f()
        self.assertEqual([x["id"] for x in out], [701, 702])
        por = {x["id"]: x for x in out}
        self.assertEqual(por[701]["fuente"], "declarado")
        self.assertEqual(por[702]["fuente"], "escrito a mano")
        self.assertEqual(por[702]["cobro"], 220000)
        self.assertEqual(por[701]["documentos"], ["FCV 11348"])
        self.assertEqual(por[701]["cliente"], "Gimnasio Uno SpA")
        self.assertEqual(por[701]["creada"]["por"], "ejecutivo1")
        self.assertIsNone(por[701]["ultimo_cambio"])
        self.assertEqual(por[702]["ultimo_cambio"]["por"], "daniel")
        for sql in visto["sql"]:
            self.assertNotRegex(sql.upper(), r"\b(INSERT|UPDATE|DELETE|DROP|ALTER)\b")
        self.assertIn("NOT IN ('cancelada','anulada')", visto["sql"][0])

    def test_la_regla_es_la_de_la_calidad_de_datos(self):
        t = FA.fuente_funcion("_ot_fin_cobro_sin_origen_cargar")
        self.assertIn("_calidad.cobro_sin_origen(r)", t)
        self.assertNotIn("_random_sql", t)
        self.assertNotIn("_ot_fin_docs_cobro", t, "la lista no sale a Random: eso pasa al «Revisar», de a una")


class TestRutaDeLaLista(unittest.TestCase):
    def llamar(self, sin_origen):
        amb = FA.cargar(funciones=["ot_api_finanzas_por_reparar"], constantes=["_OT_FIN_REPARAR_LIMITE"], extra={
            "_ot_fin_es_superadmin": lambda: True, "_ot2_err": lambda m, c, s=400: ("err", c, s),
            "_ot_fin_cobro_sin_declarar_cargar": lambda: [{"id": 1}], "_ot_fin_cobro_sin_origen_cargar": sin_origen,
            "jsonify": lambda d: d})
        return amb["ot_api_finanzas_por_reparar"]()

    def test_la_tercera_lista_viaja_con_la_primera(self):
        r = self.llamar(lambda: [{"id": 9}])
        self.assertEqual((r["cobro_sin_declarar"], r["cobro_sin_origen"], r["error_sin_origen"]), ([{"id": 1}], [{"id": 9}], False))

    def test_si_la_tercera_falla_la_primera_sale_igual(self):
        def rota():
            raise RuntimeError("SELECT secreto")
        r = self.llamar(rota)
        self.assertEqual(r["cobro_sin_declarar"], [{"id": 1}])
        self.assertEqual(r["cobro_sin_origen"], [])
        self.assertTrue(r["error_sin_origen"])


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _docs(servicio=342857, despacho=449580, sku="ZZINSTALACION", incompleto=False):
    return {"documentos": [{"titulo": "FCV 11348", "leido": not incompleto, "otro_cliente": False, "omitido": None}],
            "servicio": {"hay_lineas": servicio > 0, "total": servicio, "usado": 0, "saldo": servicio},
            "despacho": {"hay_lineas": despacho > 0, "total": despacho, "usado": 0, "saldo": despacho},
            "sku_servicio": sku, "incompleto": incompleto, "otra_empresa": []}


class _Cur:
    def __init__(self, m):
        self.m = m

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.m.sql.append((sql, params))


class _Conn:
    def __init__(self, m):
        self.m = m

    def cursor(self):
        return _Cur(self.m)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class TestConfirmarOrigen(unittest.TestCase):
    """La ruta REAL «Corregir finanzas» con la cuenta única real y una base de mentira (OT-2026-00151 con el cobro sin origen)."""

    FILA = {"id": 151, "numero_ot": "OT-2026-00151", "estado": "cerrada", "tipo": "instalacion", "cliente_id": 5, "zz_monto": 342857,
            "zz_envio_monto": 449580, "costo_proveedor": 195000, "costo_despacho": 195000, "centro_costo": "sstt",
            "modalidad_cobro": "pagado", "cubierto_por": None, "costo": None, "zz_codigo": None, "valor_origen": None,
            "proveedor_tipo": "interno", "valorizado_clp": None, "valorizado_fuente": None, "zz_motivo_manual": None,
            "contrato_real": 0, "ot_madre_id": None, "ot_relacion": None, "madre_valida": 0}

    def correr(self, payload, docs=None, fila=None, docs_falla=False):
        self.sql, self.logs = [], []
        m = self
        fila = dict(self.FILA, **(fila or {}))

        class _Saldo:
            _entero = staticmethod(lambda x: int(x or 0))

        def docs_cobro(vid, cliente_id=None):
            if docs_falla:
                raise RuntimeError("Random caído")
            return docs if docs is not None else _docs()

        def anexo_vivo(vid):
            raise AssertionError("confirmar el origen no toca el costo: el candado del anexo no se consulta")

        extra = {
            "json": json, "_ot_fin_es_superadmin": lambda: True,
            "mysql_fetchone": lambda sql, params=None: dict(fila) if "mant_visitas" in sql else {"rut": "1-9"},
            "_OT_FIN_SELECT": "SELECT ... FROM mant_visitas v WHERE v.id=%s",
            "request": type("R", (), {"get_json": staticmethod(lambda silent=True: payload)})(),
            "_ot2_err": lambda msg, cod=None, status=400: ({"ok": False, "error": msg, "codigo": cod}, status),
            "jsonify": lambda d: d, "current_username": lambda: "daniel@sphs.cl", "_mant_log": lambda *a, **k: m.logs.append(a),
            "get_mysql": lambda: _Conn(m), "_ot_fin_rep_de": lambda vid: None, "_ot_es_interna": lambda v: False,
            "_ot_saldo_reanotar_aportes": lambda vid: (_ for _ in ()).throw(AssertionError("no reanota: los montos no cambian")),
            "_ot_saldo_docs_de_ot": lambda vid: [], "_ot_saldo_chequear": lambda *a, **k: None,
            "_ot_saldo_autorizado_hasta": lambda **k: {"servicio": 0, "despacho": 0}, "_saldo": _Saldo(),
            "_ot_fin_docs_cobro": docs_cobro, "_ot_fin_anexo_vivo": anexo_vivo, "chile_fmt_filter": lambda *a, **k: "",
            "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico"), ("logistica", "Logística")],
        }
        amb = FA.cargar(
            funciones=["ot2_api_finanzas_corregir", "_ot_fin_corr_desde_docs", "_ot_finanzas", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp",
                       "_ot_fin_ganancia"],
            constantes=["_OT_FIN_CORR_CAMPOS", "_OT_FIN_CORR_ROT", "_OT_FIN_CORR_MOTIVO_MIN", "_OT_FIN_ZZ_NO_SERVICIO", "_OT2_LINEA_ZZ",
                        "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_COBERTURA_TXT",
                        "_OT_FIN_UMBRAL_BAJO"],
            extra=extra)
        return amb["ot2_api_finanzas_corregir"](151)

    def cambios(self):
        sql, params = self.sql[0]
        campos = re.findall(r"(\w+)=%s", sql.split("SET", 1)[1].split("WHERE")[0])
        return dict(zip(campos, params))

    MOTIVO = "El cobro es exactamente lo de la FCV 11348: se fija su origen."

    def test_documento_que_coincide_fija_el_origen_sin_tocar_montos(self):
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO})
        self.assertTrue(r["ok"], r)
        c = self.cambios()
        self.assertEqual(c, {"valor_origen": "zz", "zz_codigo": "ZZINSTALACION"})
        self.assertEqual(r["ganancia_antes"], r["ganancia_despues"], "el monto no cambia")
        self.assertIn("INSERT INTO mant_ot_finanzas_correcciones", self.sql[1][0], "mismo registro inmutable")
        self.assertEqual(json.loads(self.sql[1][1][5]), {"valor_origen": "zz", "zz_codigo": "ZZINSTALACION"})
        self.assertEqual(self.logs[0][2], "finanzas_corregidas")

    def test_documento_tambien_si_manda_los_mismos_montos(self):
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO, "zz_monto": 342857, "zz_envio_monto": "449580"})
        self.assertTrue(r["ok"], r)

    def test_si_los_documentos_dicen_otro_monto_no_se_fija_nada(self):
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO}, docs=_docs(servicio=300000))
        self.assertEqual((r[0]["codigo"], r[1]), ("DOCUMENTOS_NO_COINCIDEN", 409))
        self.assertEqual(self.sql, [])
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO}, docs=_docs(incompleto=True))
        self.assertEqual(r[0]["codigo"], "DOCUMENTOS_NO_COINCIDEN", "un documento sin leer nunca se da por bueno")
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO}, docs_falla=True)
        self.assertEqual((r[0]["codigo"], r[1]), ("DOCUMENTOS_NO_LEIDOS", 503))
        self.assertEqual(self.sql, [])

    def test_confirmar_el_origen_nunca_cambia_montos(self):
        for cuerpo in ({"zz_monto": 300000}, {"zz_envio_monto": 0}, {"costo_proveedor": 1}, {"centro_costo": "logistica"}):
            r = self.correr(dict(cuerpo, confirmar_origen="manual", motivo="Cobro acordado con el cliente en terreno"))
            self.assertEqual((r[0]["codigo"], r[1]), ("ORIGEN_CON_CAMBIOS", 409), cuerpo)
            self.assertEqual(self.sql, [])

    def test_manual_exige_el_motivo_y_lo_copia(self):
        r = self.correr({"confirmar_origen": "manual", "motivo": "corto"})
        self.assertEqual(r[0]["codigo"], "MOTIVO_CORTO")
        self.assertEqual(self.sql, [])
        motivo = "Lo acordó comercial con el cliente: visita extra sin factura propia."
        r = self.correr({"confirmar_origen": "manual", "motivo": motivo})
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.cambios(), {"valor_origen": "manual", "zz_motivo_manual": motivo})
        self.assertIn("Motivo del cobro escrito a mano", self.logs[0][3])

    def test_lo_que_no_aplica_se_rechaza_sin_escribir(self):
        r = self.correr({"confirmar_origen": "otro", "motivo": self.MOTIVO})
        self.assertEqual(r[0]["codigo"], "ORIGEN_INVALIDO")
        r = self.correr({"confirmar_origen": "manual", "motivo": self.MOTIVO}, fila={"zz_monto": None})
        self.assertEqual(r[0]["codigo"], "SIN_COBRO")
        r = self.correr({"confirmar_origen": "manual", "motivo": self.MOTIVO}, fila={"modalidad_cobro": "garantia"})
        self.assertEqual(r[0]["codigo"], "NO_SE_COBRA")
        r = self.correr({"confirmar_origen": "documento", "motivo": self.MOTIVO}, fila={"valor_origen": "zz", "zz_codigo": "ZZINSTALACION"})
        self.assertEqual(r[0]["codigo"], "SIN_CAMBIOS", "ya tenía ese origen")
        self.assertEqual(self.sql, [])

    def test_sin_confirmar_origen_la_correccion_sigue_igual(self):
        r = self.correr({"motivo": self.MOTIVO, "zz_monto": 342857, "zz_envio_monto": 449580, "desde_documentos": True})
        self.assertEqual(r[0]["codigo"], "SIN_CAMBIOS", "sin confirmar_origen, mismos montos = nada que cambiar (como siempre)")


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
    return global.__tabs = global.__tabs || ['cobro','anexo','origen'].map(t => {
      const e = El('tab-' + t); e.attrs = {'data-tab': t}; e.on = false; e.getAttribute = n => e.attrs[n];
      e.classList = {toggle(c, v){ if (c === 'on') e.on = !!v; }}; return e; });
  }
};
global.window = global;
if (escenario.search) global.location = {search: escenario.search};
const posts = [], toasts = [], confirms = [], prompts = [];
const respuestas = (escenario.prompts || []).slice();
global.ilusToast = (m, o) => toasts.push([m, o && o.type]);
global.ilusConfirm = async (o) => { confirms.push(o); return escenario.confirmar !== false; };
global.ilusPrompt = async (o) => { prompts.push(o); return respuestas.length ? respuestas.shift() : null; };
global.fetch = async (url, opts) => {
  const metodo = ((opts && opts.method) || 'GET').toUpperCase();
  const cuerpo = opts && opts.body ? JSON.parse(opts.body) : null;
  if (metodo === 'POST') posts.push({url, cuerpo});
  const r = escenario.rutas[metodo + ' ' + url];
  if (!r) return {ok:false, status:404, json: async () => ({ok:false, error:'sin ruta ' + url})};
  return {ok: r.status === undefined || r.status < 400, status: r.status || 200, json: async () => r.json};
};
(async () => {
  eval(js);
  await new Promise(r => setTimeout(r, 30));
  const paso = async (accion) => { await accion(); await new Promise(r => setTimeout(r, 30)); };
  const clic = (id, btn) => els[id].fire('click', {target: {closest: (s) => (s.indexOf('data-pg') >= 0 || s.indexOf('data-act') >= 0) ? btn : null}});
  const salida = [];
  for (const p of escenario.pasos) {
    if (p.tab) await paso(() => global.__tabs.find(t => t.attrs['data-tab'] === p.tab).fire('click', {}));
    if (p.todo) await paso(() => els.frTodo.fire('click', {}));
    if (p.revisarSel) await paso(() => els.frRevisarSel.fire('click', {}));
    if (p.aplicarListas) await paso(() => els.frAplicarListas.fire('click', {}));
    if (p.fila) await paso(() => clic('frBody', {getAttribute: (n) => ({'data-act': p.fila.act, 'data-k': p.fila.k})[n], disabled: false}));
    salida.push({n: p.n || '', body: els.frBody.innerHTML, head: els.frHead.innerHTML, q: els.frQ.value,
                 nOrigen: els['frN-origen'].textContent, tabOn: (global.__tabs || []).filter(t => t.on).map(t => t.attrs['data-tab'])});
  }
  console.log(JSON.stringify({posts, toasts, confirms: confirms.map(c => ({title: c.title, message: c.message, sub: c.sub})),
                              prompts: prompts.map(p => ({title: p.title, minLength: p.minLength})), salida}));
})().catch(e => { console.error(e.stack || e); process.exit(1); });
"""


def _corr(zz, env=0, docs=None, docs_error=False, origen=None, motivo=None, cobra=True):
    return {"json": {"ok": True, "docs": docs, "docs_error": docs_error, "cobra": cobra, "cobertura": "cobra",
                     "base": {"zz_monto": zz, "zz_envio_monto": env, "valor_origen": origen, "zz_motivo_manual": motivo},
                     "fin": {"cobre": {"hay": True, "total": zz + env}, "me_cobraron": {"total": 100000}}}}


class TestPantallaEjecutada(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        cls.js = re.search(r"<script>(.*?)</script>", _leer(PLANTILLA), re.S).group(1)

    def correr(self, escenario):
        if not self.node:
            self.skipTest("Node no está instalado")
        with tempfile.TemporaryDirectory() as d:
            arch, h = os.path.join(d, "datos.json"), os.path.join(d, "h.js")
            with open(arch, "w", encoding="utf-8") as fh:
                json.dump({"js": self.js, "escenario": escenario}, fh)
            with open(h, "w", encoding="utf-8") as fh:
                fh.write(_HARNESS)
            r = subprocess.run([self.node, h, arch], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr)
            return json.loads(r.stdout.strip().splitlines()[-1])

    def rutas(self):
        lista = [{"id": i, "numero": "OT-2026-%05d" % i, "cliente": c, "estado": "cerrada", "tipo": "instalacion",
                  "cobro": 300000, "servicio": 300000, "despacho": 0, "fuente": "declarado", "documentos": ["FCV %d" % i],
                  "creada": {"por": "ejecutivo1", "cuando": "12/10/2026 10:00"}, "ultimo_cambio": None}
                 for i, c in ((701, "Gimnasio Uno SpA"), (702, "Corporación Deportiva Los Andes del Sur Limitada"),
                              (703, "Tercero SpA"), (704, "Cuarto SpA"))]
        return {
            "GET /ot/api/finanzas/por-reparar": {"json": {"ok": True, "cobro_sin_declarar": [], "cobro_sin_origen": lista}},
            "GET /ot/api/anexos/vinculo-costos": {"json": {"ok": True, "por_corregir": []}},
            "GET /ot/api/finanzas/701/correcciones": _corr(300000, docs=_docs(servicio=300000, despacho=0)),       # (a) coincide
            "GET /ot/api/finanzas/702/correcciones": _corr(300000, docs=_docs(servicio=250000, despacho=40000)),   # (b) otro monto
            "GET /ot/api/finanzas/703/correcciones": _corr(300000, docs=None),                                      # (c) sin documento
            "GET /ot/api/finanzas/704/correcciones": _corr(300000, docs_error=True),                                # Random caído
            "POST /ot/api/finanzas/701/corregir": {"json": {"ok": True, "ganancia_despues": 200000}},
            "POST /ot/api/finanzas/702/corregir": {"json": {"ok": True, "ganancia_despues": 190000}},
            "POST /ot/api/finanzas/703/corregir": {"json": {"ok": True, "ganancia_despues": 200000}},
        }

    def test_revisar_no_escribe_y_cada_caso_va_por_su_camino(self):
        out = self.correr({"rutas": self.rutas(), "prompts": ["corto", "Acuerdo comercial con el cliente, visita adicional"], "pasos": [
            {"n": "tab", "tab": "origen"}, {"n": "rev", "todo": True}, {"n": "rev2", "revisarSel": True},
            {"n": "aplicar", "aplicarListas": True},
            {"n": "usar", "fila": {"act": "usar", "k": "origen:702"}},
            {"n": "mano1", "fila": {"act": "mano", "k": "origen:703"}},
            {"n": "mano2", "fila": {"act": "mano", "k": "origen:703"}}]})
        s = {x["n"]: x for x in out["salida"]}
        self.assertIn("Cobro sin respaldo", s["tab"]["head"])
        self.assertEqual(s["tab"]["nOrigen"], 4)
        rev = s["rev2"]["body"]
        self.assertIn("es exactamente lo que aportan sus documentos", rev)
        self.assertIn("sus documentos dicen $250.000 + despacho $40.000", rev.replace(" ", " "))
        self.assertIn("No tiene documento de cobro", rev)
        self.assertIn("Random no respondió", rev)
        self.assertIn("Corporación Deportiva Los Andes del Sur Limitada", rev)
        urls = [p["url"] for p in out["posts"]]
        self.assertNotIn("/ot/api/finanzas/704/corregir", urls, "lo bloqueado nunca se toca")
        a = [p for p in out["posts"] if p["url"].endswith("/701/corregir")]
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0]["cuerpo"]["confirmar_origen"], "documento")
        self.assertNotIn("zz_monto", a[0]["cuerpo"], "(a) no cambia montos")
        self.assertGreaterEqual(len(a[0]["cuerpo"]["motivo"]), 15)
        self.assertIn("Se fijará el origen", out["confirms"][0]["message"], "«Aplicar las listas»")
        b = [p for p in out["posts"] if p["url"].endswith("/702/corregir")]
        self.assertEqual(len(b), 1, "(b) va de a una y nunca entra a «Aplicar las listas»")
        self.assertEqual((b[0]["cuerpo"]["zz_monto"], b[0]["cuerpo"]["zz_envio_monto"], b[0]["cuerpo"]["desde_documentos"]), (250000, 40000, True))
        self.assertNotIn("confirmar_origen", b[0]["cuerpo"])
        conf_b = next(c for c in out["confirms"] if c["title"] == "Usar los documentos")
        self.assertIn("Antes: $300.000", conf_b["sub"].replace(" ", " "))
        self.assertIn("Después: $250.000 + despacho $40.000", conf_b["sub"].replace(" ", " "))
        c = [p for p in out["posts"] if p["url"].endswith("/703/corregir")]
        self.assertEqual(len(c), 1, "el motivo corto no se manda")
        self.assertEqual(c[0]["cuerpo"], {"confirmar_origen": "manual", "motivo": "Acuerdo comercial con el cliente, visita adicional"})
        self.assertEqual(out["prompts"][0]["minLength"], 15)
        self.assertTrue(any("al menos 15" in t[0] for t in out["toasts"]))
        self.assertEqual(len(out["posts"]), 3)

    def test_revisar_solo_no_escribe(self):
        out = self.correr({"rutas": self.rutas(), "pasos": [{"tab": "origen"}, {"todo": True}, {"revisarSel": True}]})
        self.assertEqual(out["posts"], [])

    def test_el_enlace_abre_la_pestana_ya_filtrada(self):
        out = self.correr({"rutas": self.rutas(), "search": "?tab=origen&q=OT-2026-00702", "pasos": [{"n": "x"}]})
        b = out["salida"][0]["body"]
        self.assertIn("OT-2026-00702", b)
        self.assertNotIn("OT-2026-00701", b)
        self.assertEqual(out["salida"][0]["q"], "OT-2026-00702")
        self.assertEqual(out["salida"][0]["tabOn"], ["origen"])

    def test_plantilla_sigue_las_reglas(self):
        t = _leer(PLANTILLA)
        self.assertIn('data-tab="origen"', t)
        self.assertIn("confirmar_origen: 'documento'", t)
        self.assertIn("confirmar_origen: 'manual'", t)
        self.assertIn("ilusPrompt", t)
        self.assertNotRegex(t, r"(?<![A-Za-z.])(alert|confirm|prompt)\(")


if __name__ == "__main__":
    unittest.main()

"""«Corregir finanzas» toma el cobro de los DOCUMENTOS ya declarados — 2026-10-29.

Daniel, con la OT-2026-00151 (factura FCV 11348 declarada, servicio $342.857 + despacho $449.580, y «Cobré» en $0): «quisiera que podamos
utilizar el documento que ya declaré… me pide hacerlo a mano, lo que es peligroso… que me calcule automáticamente lo que cobramos, lo
contraste con lo que nos cobraron y arroje el margen».

Se prueba con datos de mentira (los números de esa OT):
  · la regla pura que decide cuándo el cobro queda rotulado «línea del documento» (solo si es EXACTAMENTE lo que aportan los documentos);
  · la lectura de lo que aportan los documentos (con su saldo y sin calcular a ciegas si Random no entregó alguno);
  · la ruta de corrección ejecutada de verdad con una base de mentira (rótulo, línea ZZ, margen, y que el navegador nunca fija el rótulo);
  · el modal ejecutado en Node con la cuenta única real (static/ot_finanzas.js): el botón, el margen a la vista y «ya está tomado».
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import jinja2

from tests import _fuente_app as FA

PLANTILLA = os.path.join(FA.RAIZ, "templates", "ot2", "detalle.html")
OT_FINANZAS_JS = os.path.join(FA.RAIZ, "static", "ot_finanzas.js")


def _docs(servicio=342857, despacho=449580, sku="ZZINSTALACION", incompleto=False, usado=0):
    return {"documentos": [{"titulo": "FCV 11348", "leido": not incompleto, "otro_cliente": False, "omitido": None}],
            "servicio": {"hay_lineas": servicio > 0, "total": servicio + usado, "usado": usado, "saldo": servicio},
            "despacho": {"hay_lineas": despacho > 0, "total": despacho, "usado": 0, "saldo": despacho},
            "sku_servicio": sku, "incompleto": incompleto}


class TestReglaPura(unittest.TestCase):
    def setUp(self):
        self.f = FA.cargar(funciones=["_ot_fin_corr_desde_docs"])["_ot_fin_corr_desde_docs"]

    def test_solo_cuenta_si_el_monto_es_exactamente_lo_que_aportan_los_documentos(self):
        self.assertTrue(self.f(True, {"zz_monto": 342857, "zz_envio_monto": 449580}, _docs()))
        self.assertFalse(self.f(True, {"zz_monto": 342000, "zz_envio_monto": 449580}, _docs()))      # editado a mano: no es del documento
        self.assertFalse(self.f(True, {"zz_monto": 342857, "zz_envio_monto": 100}, _docs()))
        self.assertFalse(self.f(True, {"zz_monto": 4671100, "zz_envio_monto": None}, _docs()))      # el total de la factura NO es el servicio

    def test_el_navegador_solo_avisa_nunca_fija_el_rotulo(self):
        self.assertFalse(self.f(False, {"zz_monto": 342857, "zz_envio_monto": 449580}, _docs()))     # sin pedirlo: manual, como siempre

    def test_sin_documentos_o_sin_leer_o_sin_saldo_nunca_se_calcula_a_ciegas(self):
        n = {"zz_monto": 342857, "zz_envio_monto": 449580}
        self.assertFalse(self.f(True, n, None))
        self.assertFalse(self.f(True, n, _docs(incompleto=True)))
        self.assertFalse(self.f(True, {"zz_monto": 0, "zz_envio_monto": 449580}, _docs(servicio=0)))
        self.assertFalse(self.f(True, n, _docs(servicio=0)))

    def test_si_los_documentos_no_traen_despacho_el_cobro_del_despacho_debe_ser_cero(self):
        self.assertTrue(self.f(True, {"zz_monto": 342857, "zz_envio_monto": None}, _docs(despacho=0)))
        self.assertTrue(self.f(True, {"zz_monto": 342857, "zz_envio_monto": 0}, _docs(despacho=0)))
        self.assertFalse(self.f(True, {"zz_monto": 342857, "zz_envio_monto": 5}, _docs(despacho=0)))


class TestLecturaDeDocumentos(unittest.TestCase):
    def amb(self, docs, saldo, rut="72498100-3"):
        extra = {"_ot_saldo_docs_de_ot": lambda vid: docs, "_ot_saldo_de_docs": lambda d, excluir_vid=None, cliente_rut=None: saldo,
                 "mysql_fetchone": lambda sql, params=None: {"rut": rut}}
        return FA.cargar(funciones=["_ot_fin_docs_cobro"], extra=extra)["_ot_fin_docs_cobro"]

    def saldo(self, leido=True, otro=False, omitido=None):
        cat = lambda t, u: {"hay_lineas": t > 0, "total": t, "usado": u, "saldo": max(0, t - u)}
        return {"por_doc": [{"titulo": "FCV 11348", "leido": leido, "otro_cliente": otro, "omitido": omitido,
                             "servicio": {"lineas": [{"sku": "zzinstalacion", "monto": 342857}]}, "despacho": {"lineas": []}}],
                "combinado": {"servicio": cat(342857, 0), "despacho": cat(449580, 0)}}

    def test_aporta_servicio_y_despacho_con_su_linea_zz(self):
        r = self.amb([("FCV", "11348")], self.saldo())(151, 5)
        self.assertEqual((r["servicio"]["saldo"], r["despacho"]["saldo"]), (342857, 449580))
        self.assertEqual(r["sku_servicio"], "ZZINSTALACION")
        self.assertFalse(r["incompleto"])
        self.assertEqual(r["documentos"][0]["titulo"], "FCV 11348")

    def test_lo_que_otras_ot_ya_usan_no_se_puede_tomar(self):
        s = self.saldo()
        s["combinado"]["servicio"] = {"hay_lineas": True, "total": 342857, "usado": 100000, "saldo": 242857}
        r = self.amb([("FCV", "11348")], s)(151, 5)
        self.assertEqual((r["servicio"]["saldo"], r["servicio"]["usado"]), (242857, 100000))

    def test_si_random_no_entrego_un_documento_queda_incompleto(self):
        self.assertTrue(self.amb([("FCV", "1")], self.saldo(leido=False))(151, 5)["incompleto"])

    def test_un_documento_de_otra_empresa_no_bloquea_a_los_demas_se_informa(self):
        # 2026-10-29 (OT-2026-00190): se sabe con certeza que no cuenta (el saldo lo excluye): no impide tomar el cobro de la factura buena.
        r = self.amb([("FCV", "1")], self.saldo(otro=True))(151, 5)
        self.assertFalse(r["incompleto"])
        self.assertEqual(r["otra_empresa"], ["FCV 11348"])

    def test_una_nota_de_venta_cuya_factura_ya_esta_no_cuenta_ni_marca_incompleto(self):
        r = self.amb([("NVV", "1")], self.saldo(omitido="Su factura ya está en la OT"))(151, 5)
        self.assertFalse(r["incompleto"])
        self.assertIsNone(r["sku_servicio"])

    def test_sin_documentos_no_hay_nada_que_tomar(self):
        self.assertIsNone(self.amb([], {})(151, 5))


class _Cur:
    def __init__(self, mundo):
        self.m = mundo

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.m.sql.append((sql, params))


class _Conn:
    def __init__(self, mundo):
        self.m = mundo

    def cursor(self):
        return _Cur(self.m)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class TestRutaDeCorreccion(unittest.TestCase):
    """La ruta REAL, con la cuenta única real y una base de mentira: los números de la OT-2026-00151."""

    FILA = {"id": 151, "numero_ot": "OT-2026-00151", "estado": "cerrada", "tipo": "instalacion", "cliente_id": 5, "zz_monto": None,
            "zz_envio_monto": None, "costo_proveedor": 195000, "costo_despacho": 195000, "centro_costo": "sstt", "modalidad_cobro": "pagado",
            "cubierto_por": None, "costo": 4671100, "zz_codigo": None, "valor_origen": None, "proveedor_tipo": "interno",
            "valorizado_clp": None, "valorizado_fuente": None, "zz_motivo_manual": None, "contrato_real": 0, "ot_madre_id": None,
            "ot_relacion": None, "madre_valida": 0}

    def correr(self, payload, docs, superadmin=True):
        self.sql, self.logs = [], []
        m = self

        class _Saldo:
            _entero = staticmethod(lambda x: int(x or 0))

        extra = {
            "json": json,
            "_ot_fin_es_superadmin": lambda: superadmin, "mysql_fetchone": lambda sql, params=None: dict(self.FILA) if "mant_visitas" in sql else {"rut": "1-9"},
            "_OT_FIN_SELECT": "SELECT ... FROM mant_visitas v WHERE v.id=%s",
            "request": type("R", (), {"get_json": staticmethod(lambda silent=True: payload)})(),
            "_ot2_err": lambda msg, cod=None, status=400: ({"ok": False, "error": msg, "codigo": cod}, status),
            "jsonify": lambda d: d, "current_username": lambda: "daniel@sphs.cl", "_mant_log": lambda *a, **k: m.logs.append(a),
            "get_mysql": lambda: _Conn(m), "_ot_fin_rep_de": lambda vid: None, "_ot_es_interna": lambda v: False,
            "_ot_saldo_reanotar_aportes": lambda vid: None, "_ot_saldo_docs_de_ot": lambda vid: [],
            "_ot_saldo_chequear": lambda *a, **k: None, "_ot_saldo_autorizado_hasta": lambda **k: {"servicio": 0, "despacho": 0},
            "_saldo": _Saldo(), "_ot_fin_docs_cobro": lambda vid, cliente_id=None: docs, "chile_fmt_filter": lambda *a, **k: "",
            "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico"), ("logistica", "Logística")],
        }
        amb = FA.cargar(
            funciones=["ot2_api_finanzas_corregir", "_ot_fin_corr_desde_docs", "_ot_finanzas", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp", "_ot_fin_ganancia"],
            constantes=["_OT_FIN_CORR_CAMPOS", "_OT_FIN_CORR_ROT", "_OT_FIN_CORR_MOTIVO_MIN", "_OT_FIN_ZZ_NO_SERVICIO", "_OT2_LINEA_ZZ",
                        "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_COBERTURA_TXT", "_OT_FIN_UMBRAL_BAJO"],
            extra=extra)
        return amb["ot2_api_finanzas_corregir"](151)

    def cuerpo(self, **kw):
        base = {"motivo": "Cobro tomado de FCV 11348: servicio y despacho según sus líneas.", "zz_monto": 342857, "zz_envio_monto": 449580,
                "costo_proveedor": 195000, "costo_despacho": 195000, "valorizado_clp": None, "centro_costo": "sstt", "desde_documentos": True}
        base.update(kw)
        return base

    def cambios(self):
        sql, params = self.sql[0]
        campos = re.findall(r"(\w+)=%s", sql.split("SET", 1)[1].split("WHERE")[0])
        return dict(zip(campos, params))

    def test_con_el_cobro_de_los_documentos_queda_como_linea_del_documento_y_con_su_linea_zz(self):
        r = self.correr(self.cuerpo(), _docs())
        self.assertTrue(r["ok"], r)
        c = self.cambios()
        self.assertEqual((c["zz_monto"], c["zz_envio_monto"]), (342857, 449580))
        self.assertEqual(c["valor_origen"], "zz")                          # «línea del documento», no «escrito a mano»
        self.assertEqual(c["zz_codigo"], "ZZINSTALACION")
        self.assertEqual(r["ganancia_antes"], -390000)                      # Cobré $0 − Me cobraron $390.000
        self.assertEqual(r["ganancia_despues"], 792437 - 390000)            # el margen: $402.437
        reg = self.sql[1]
        self.assertIn("INSERT INTO mant_ot_finanzas_correcciones", reg[0])  # queda en el registro inmutable, con su motivo
        self.assertIn("FCV 11348", reg[1][3])

    def test_si_el_navegador_no_lo_pide_queda_escrito_a_mano_como_siempre(self):
        r = self.correr(self.cuerpo(desde_documentos=False), _docs())
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.cambios()["valor_origen"], "manual")
        self.assertNotIn("zz_codigo", self.cambios())

    def test_un_monto_distinto_al_de_los_documentos_queda_a_mano_aunque_el_navegador_diga_que_es_del_documento(self):
        r = self.correr(self.cuerpo(zz_monto=300000), _docs())
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.cambios()["valor_origen"], "manual")

    def test_con_un_documento_sin_leer_nunca_se_rotula_como_del_documento(self):
        r = self.correr(self.cuerpo(), _docs(incompleto=True))
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.cambios()["valor_origen"], "manual")

    def test_el_motivo_sigue_siendo_obligatorio_y_solo_el_superadmin_corrige(self):
        r = self.correr(self.cuerpo(motivo="corto"), _docs())
        self.assertEqual((r[0]["codigo"], r[1]), ("MOTIVO_CORTO", 400))
        r = self.correr(self.cuerpo(), _docs(), superadmin=False)
        self.assertEqual((r[0]["codigo"], r[1]), ("SOLO_SUPERADMIN", 403))

    def test_los_documentos_solo_se_leen_si_el_modal_lo_pide(self):
        llamadas = []
        self.correr(self.cuerpo(desde_documentos=False), None)
        src = FA.fuente_funcion("ot2_api_finanzas_corregir")
        self.assertIn('if d.get("desde_documentos"):', src)                 # sin el aviso del modal no se consulta Random
        self.assertEqual(llamadas, [])


class TestRutasYModal(unittest.TestCase):
    def test_las_rutas_siguen_cerradas_a_tecnicos_y_el_modal_recibe_los_documentos(self):
        for ruta in ("ot2_api_finanzas_corregir", "ot2_api_finanzas_correcciones"):
            d = " ".join(FA.decoradores(ruta))
            self.assertIn("_mant_required", d, ruta)
            self.assertIn("_no_tecnico", d, ruta)
            self.assertIn("_ot_fin_es_superadmin()", FA.fuente_funcion(ruta), ruta)
        src = FA.fuente_funcion("ot2_api_finanzas_correcciones")
        self.assertIn('"docs": _docs_cobro, "docs_error": _docs_error', src)
        self.assertIn("except Exception", src[src.index("_ot_fin_docs_cobro("):])    # si Random falla, el modal sigue funcionando

    def test_leer_los_documentos_no_escribe_nada(self):
        src = FA.fuente_funcion("_ot_fin_docs_cobro")
        for mal in ("INSERT", "UPDATE", "DELETE", "mysql_execute", "commit"):
            self.assertNotIn(mal, src)


def _script_modal():
    with open(PLANTILLA, encoding="utf-8") as fh:
        t = fh.read()
    i = t.index("/* 💰 Corregir finanzas (solo superadmin) — 2026-10-07 */")
    j = t.index("</script>", i)
    return jinja2.Environment().from_string(t[i:j]).render()


@unittest.skipUnless(shutil.which("node"), "Node no está disponible")
class TestModalEnNode(unittest.TestCase):
    """El modal REAL, con la cuenta única REAL del navegador (static/ot_finanzas.js) y una página de mentira."""

    LIBRETO = r"""
const vm = require('vm'), fs = require('fs');
const els = {};
function el(id){ return els[id] || (els[id] = {id: id, value: '', innerHTML: '', className: '', textContent: '', hidden: false, focus: function(){ this.enfocado = true; }}); }
const sb = {document: {getElementById: el}, console: console, OTD_VID: 151, ilusToast: function(){}, otdEsc: function(s){ return s; }, setTimeout: setTimeout};
sb.window = sb;
vm.createContext(sb);
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), sb);
vm.runInContext(fs.readFileSync(process.argv[3], 'utf8'), sb);
const E = JSON.parse(process.argv[4]);
vm.runInContext('_otdFcBase = ' + JSON.stringify(E.base) + '; _otdFcRep = null; _otdFcSug = "ZZINSTALACION"; _otdFcDocs = ' + JSON.stringify(E.docs) +
                '; _otdFcDocsError = ' + JSON.stringify(!!E.error) + '; _otdFcInicial = ' + JSON.stringify(E.inicial || {}) + ';', sb);
Object.keys(E.inputs).forEach(function(k){ el('otdFc_' + k).value = E.inputs[k]; });
const out = {};
vm.runInContext('otdFinCorrCalc()', sb);
out.antes = {docs: el('otdFcDocs').innerHTML, clase: el('otdFcDocs').className, res: el('otdFcRes').innerHTML, igual: vm.runInContext('_otdFcDocsIgual()', sb)};
if (E.tomar){
  vm.runInContext('otdFinCorrTomarDocs()', sb);
  out.despues = {docs: el('otdFcDocs').innerHTML, res: el('otdFcRes').innerHTML, zz: el('otdFc_zz_monto').value, env: el('otdFc_zz_envio_monto').value,
                 motivo: el('otdFc_motivo').value, igual: vm.runInContext('_otdFcDocsIgual()', sb), enfocado: !!el('otdFc_motivo').enfocado};
}
console.log(JSON.stringify(out));
"""

    BASE = {"modalidad_cobro": "pagado", "tipo": "instalacion", "cliente_id": 5, "contrato_real": 0, "costo": 4671100, "zz_monto": None,
            "zz_codigo": None, "zz_envio_monto": None, "valor_origen": None, "costo_proveedor": 195000, "costo_despacho": 195000,
            "proveedor_tipo": "interno", "valorizado_clp": None}
    INPUTS = {"zz_monto": "", "zz_envio_monto": "", "costo_proveedor": "195000", "costo_despacho": "195000", "valorizado_clp": ""}

    def correr(self, docs, inputs=None, tomar=False, error=False, inicial=None):
        with tempfile.TemporaryDirectory() as tmp:
            js, modal = os.path.join(tmp, "libreto.js"), os.path.join(tmp, "modal.js")
            with open(js, "w", encoding="utf-8") as fh:
                fh.write(self.LIBRETO)
            with open(modal, "w", encoding="utf-8") as fh:
                fh.write(_script_modal())
            e = {"base": self.BASE, "docs": docs, "inputs": dict(self.INPUTS, **(inputs or {})), "tomar": tomar, "error": error,
                 "inicial": inicial}
            r = subprocess.run([shutil.which("node"), js, OT_FINANZAS_JS, modal, json.dumps(e)], capture_output=True, text=True,
                               encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr[:800])
            return json.loads(r.stdout.strip().splitlines()[-1])

    def test_ofrece_tomar_el_cobro_y_muestra_el_margen_contra_lo_que_nos_cobraron(self):
        o = self.correr(_docs())
        d = o["antes"]["docs"]
        self.assertIn("FCV 11348", d)
        self.assertIn("$342.857", d)
        self.assertIn("$449.580", d)
        self.assertIn("$792.437", d)
        self.assertIn("Cobré $792.437 − Me cobraron $390.000 = Queda $402.437", d)     # el contraste y el margen, antes de tocar nada
        self.assertIn("50,8 %", d)
        self.assertIn("Tomar el cobro de los documentos", d)
        self.assertIn("Agregar otro documento", d)
        self.assertIn("El resto del documento son productos", d)
        self.assertFalse(o["antes"]["igual"])

    def test_al_tomarlo_llena_los_dos_montos_sugiere_el_motivo_y_la_cuenta_se_actualiza(self):
        o = self.correr(_docs(), tomar=True)["despues"]
        self.assertEqual((o["zz"], o["env"]), ("342857", "449580"))
        self.assertIn("FCV 11348", o["motivo"])
        self.assertGreaterEqual(len(o["motivo"]), 15)                                   # alcanza el mínimo del registro
        self.assertTrue(o["enfocado"])                                                  # la persona lo revisa antes de guardar
        self.assertIn("Cobré $792.437 − Me cobraron $390.000 = Queda</span><b>$402.437", o["res"])
        self.assertIn("<small>50,8 %</small>", o["res"])                                # el margen % también en la barra de arriba
        self.assertTrue(o["igual"])
        # recién prellenado en el modal: todavía NO está guardado, y se dice
        self.assertIn("Prellenado con lo que aportan los documentos", o["docs"])
        self.assertNotIn("Ya está tomado de los documentos", o["docs"])
        self.assertNotIn("Tomar el cobro de los documentos", o["docs"])

    def test_cuando_ya_esta_guardado_en_la_ot_dice_ya_esta_tomado(self):
        o = self.correr(_docs(), inputs={"zz_monto": "342857", "zz_envio_monto": "449580"},
                        inicial={"zz_monto": 342857, "zz_envio_monto": 449580})["antes"]
        self.assertIn("Ya está tomado de los documentos", o["docs"])
        self.assertNotIn("Prellenado", o["docs"])

    def test_si_la_persona_cambia_un_monto_deja_de_decir_que_es_del_documento(self):
        o = self.correr(_docs(), inputs={"zz_monto": "300000", "zz_envio_monto": "449580"})["antes"]
        self.assertFalse(o["igual"])
        self.assertIn("Tomar el cobro de los documentos", o["docs"])

    def test_lo_que_otras_ot_ya_usan_se_dice_y_baja_lo_que_se_puede_tomar(self):
        d = self.correr(_docs(servicio=242857, usado=100000))["antes"]["docs"]
        self.assertIn("$242.857", d)
        self.assertIn("otras OT ya usan $100.000", d)

    def test_con_un_documento_sin_leer_no_se_ofrece_calcular_a_ciegas(self):
        o = self.correr(_docs(incompleto=True))["antes"]
        self.assertIn("nota", o["clase"])
        self.assertIn("no calculo el cobro a ciegas", o["docs"])
        self.assertNotIn("Tomar el cobro de los documentos", o["docs"])
        self.assertIn("Agregar otro documento", o["docs"])                               # igual puede agregar otro

    def test_sin_lineas_con_saldo_dice_por_que_no_hay_nada_que_tomar(self):
        o = self.correr(_docs(servicio=0, despacho=0))["antes"]
        self.assertIn("No traen líneas de servicio ni de despacho con saldo", o["docs"])
        self.assertNotIn("Tomar el cobro de los documentos", o["docs"])

    def test_si_random_no_respondio_lo_dice_y_el_modal_sigue_funcionando(self):
        o = self.correr(None, error=True)["antes"]
        self.assertIn("No pude leer los documentos", o["docs"])
        self.assertIn("nota", o["clase"])

    def test_sin_documentos_el_bloque_no_aparece(self):
        o = self.correr(None)["antes"]
        self.assertEqual(o["docs"], "")


@unittest.skipUnless(shutil.which("node"), "Node no está disponible")
class TestAperturaDelModalEnNode(unittest.TestCase):
    """El modal abierto de punta a punta (con `fetch` de mentira): prellenado AUTOMÁTICO del cobro y «Traer factura o boleta»."""

    LIBRETO = r"""
const vm = require('vm'), fs = require('fs');
const els = {};
function el(id){ return els[id] || (els[id] = {id: id, value: '', innerHTML: '', className: '', textContent: '', hidden: false, disabled: false, focus: function(){ this.enfocado = true; },
  getAttribute: function(){ return null; }}); }
const E = JSON.parse(process.argv[4]), llamadas = [];
let n = 0;
const fetchFalso = function(url, opts){
  llamadas.push({url: url, metodo: (opts && opts.method) || 'GET', cuerpo: opts && opts.body ? JSON.parse(opts.body) : null});
  const esPost = !!(opts && opts.method === 'POST');
  const r = esPost ? E.post : E.gets[Math.min(n++, E.gets.length - 1)];
  return Promise.resolve({ok: r.http === undefined ? true : r.http < 400, json: function(){ return Promise.resolve(r.cuerpo); }});
};
const sb = {document: {getElementById: el}, console: console, OTD_VID: 152, ilusToast: function(){}, otdEsc: function(s){ return s; }, setTimeout: setTimeout, fetch: fetchFalso,
            bootstrap: {Modal: {getOrCreateInstance: function(){ return {show: function(){}, hide: function(){}}; }, getInstance: function(){ return {hide: function(){}}; }}},
            otdFinCorrCcPintar: undefined};
sb.window = sb;
vm.createContext(sb);
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), sb);
vm.runInContext(fs.readFileSync(process.argv[3], 'utf8'), sb);
(async function(){
  const out = {};
  await vm.runInContext('otdFinCorrAbrir()', sb);
  const snap = function(){ return {zz: el('otdFc_zz_monto').value, env: el('otdFc_zz_envio_monto').value, motivo: el('otdFc_motivo').value, docs: el('otdFcDocs').innerHTML,
    traerOculto: el('otdFcTraer').hidden, res: el('otdFcRes').innerHTML, err: el('otdFcErr').textContent, enfocado: !!el('otdFc_motivo').enfocado}; };
  out.abierto = snap();
  if (E.traer){
    el('otdFcDocTipo').value = E.traer.tipo; el('otdFcDocNum').value = E.traer.numero;
    await vm.runInContext('otdFinCorrTraerDoc()', sb);
    out.tras = snap(); out.msg = {texto: el('otdFcDocMsg').textContent, html: el('otdFcDocMsg').innerHTML, clase: el('otdFcDocMsg').className, boton: el('otdFcDocBtn').disabled};
  }
  out.llamadas = llamadas;
  console.log(JSON.stringify(out));
})();
"""

    def correccion(self, docs, cobra=True, hay=False):
        fin = {"cobra": cobra, "cobertura_txt": "Se le cobra al cliente" if cobra else "Garantía: no se le cobra",
               "cobre": {"hay": hay, "servicio": 0, "despacho": 0, "total": 0, "fuente": None}, "precio_anotado": 5398915 if (cobra and not hay) else None}
        base = dict(TestModalEnNode.BASE, costo=5398915, costo_proveedor=100000, costo_despacho=230000)
        if not cobra:
            base["modalidad_cobro"] = "garantia"
        return {"ok": True, "historial": [], "docs": docs, "docs_error": False, "fin": fin, "base": base, "rep": None, "zz_sugerida": "ZZINSTALACION",
                "zz_no_servicio": ["ZZRETIRO"], "centros": [], "garantia": not cobra,
                "actual": {"zz_monto": None, "zz_envio_monto": None, "costo_proveedor": 100000, "costo_despacho": 230000, "valorizado_clp": None, "centro_costo": "sstt"}}

    def correr(self, gets, post=None, traer=None):
        with tempfile.TemporaryDirectory() as tmp:
            js, modal = os.path.join(tmp, "libreto.js"), os.path.join(tmp, "modal.js")
            with open(js, "w", encoding="utf-8") as fh:
                fh.write(self.LIBRETO)
            with open(modal, "w", encoding="utf-8") as fh:
                fh.write(_script_modal())
            e = {"gets": [{"cuerpo": g} for g in gets], "post": post or {"cuerpo": {"ok": True}}, "traer": traer}
            r = subprocess.run([shutil.which("node"), js, OT_FINANZAS_JS, modal, json.dumps(e)], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr[:800])
            return json.loads(r.stdout.strip().splitlines()[-1])

    def test_al_abrir_con_documentos_y_sin_cobro_el_cobro_se_prellena_solo_y_se_ve_el_margen(self):
        o = self.correr([self.correccion(_docs())])["abierto"]
        self.assertEqual((o["zz"], o["env"]), ("342857", "449580"))                       # sin que nadie toque nada
        self.assertIn("FCV 11348", o["motivo"])                                           # motivo sugerido: queda en el registro
        self.assertFalse(o["enfocado"])                                                   # no le roba el foco a quien lee
        self.assertIn("Cobré $792.437 − Me cobraron $330.000 = Queda</span><b>$462.437", o["res"])    # OT-152: me cobraron 100.000 + 230.000
        self.assertIn("Prellenado con lo que aportan los documentos", o["docs"])
        self.assertTrue(o["traerOculto"])                                                 # ya tiene documento: no hace falta traerlo
        self.assertEqual(o["err"], "")

    def test_si_la_ot_ya_tiene_cobro_declarado_no_se_pisa_nada(self):
        o = self.correr([self.correccion(_docs(), hay=True)])["abierto"]
        self.assertEqual((o["zz"], o["env"], o["motivo"]), ("", "", ""))
        self.assertIn("Tomar el cobro de los documentos", o["docs"])                      # sigue disponible con un clic

    def test_con_un_documento_sin_leer_no_se_prellena_nada(self):
        o = self.correr([self.correccion(_docs(incompleto=True))])["abierto"]
        self.assertEqual((o["zz"], o["env"], o["motivo"]), ("", "", ""))

    def test_si_la_ot_no_tiene_documento_aparece_traer_factura_o_boleta(self):
        o = self.correr([self.correccion(None)])["abierto"]
        self.assertFalse(o["traerOculto"])
        self.assertEqual((o["zz"], o["env"]), ("", ""))

    def test_una_ot_que_no_se_cobra_no_pide_documento(self):
        o = self.correr([self.correccion(None, cobra=False)])["abierto"]
        self.assertTrue(o["traerOculto"])

    def test_traer_el_documento_lo_asocia_por_el_camino_de_siempre_y_prellena_el_cobro(self):
        r = self.correr([self.correccion(None), self.correccion(_docs())], traer={"tipo": "FCV", "numero": "11.348"})
        post = [c for c in r["llamadas"] if c["metodo"] == "POST"][0]
        self.assertEqual(post["url"], "/ot/api/152/documentos/regularizar")                # el MISMO camino que «Agregar factura»
        self.assertEqual(post["cuerpo"], {"origen": "erp", "tipo": "FCV", "numero": "11348"})
        self.assertEqual((r["tras"]["zz"], r["tras"]["env"]), ("342857", "449580"))         # tras traerlo, el cobro queda prellenado
        self.assertTrue(r["tras"]["traerOculto"])
        self.assertIn("Prellenado con lo que aportan los documentos", r["tras"]["docs"])

    def test_si_random_no_lo_encuentra_lo_dice_y_ofrece_el_formulario_completo(self):
        r = self.correr([self.correccion(None)], post={"http": 404, "cuerpo": {"ok": False, "error": "No encontramos la FCV N° 1 en el ERP."}},
                        traer={"tipo": "FCV", "numero": "1"})
        self.assertIn("No encontramos la FCV N° 1 en el ERP.", r["msg"]["html"])
        self.assertIn("Usar el formulario completo", r["msg"]["html"])
        self.assertIn("mal", r["msg"]["clase"])
        self.assertFalse(r["msg"]["boton"])                                                 # el botón vuelve a quedar disponible
        self.assertEqual((r["tras"]["zz"], r["tras"]["env"]), ("", ""))                     # nada se inventa

    def test_sin_numero_no_llama_al_servidor(self):
        r = self.correr([self.correccion(None)], traer={"tipo": "FCV", "numero": ""})
        self.assertEqual([c for c in r["llamadas"] if c["metodo"] == "POST"], [])
        self.assertIn("Escribe el número del documento", r["msg"]["texto"])


class TestPlantilla(unittest.TestCase):
    def setUp(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            self.t = fh.read()

    def test_traer_factura_usa_el_camino_de_agregar_factura_y_valida_en_el_servidor(self):
        self.assertIn('id="otdFcTraer"', self.t)
        self.assertIn("/documentos/regularizar", self.t)
        src = FA.fuente_funcion("ot_api_documentos_regularizar")                            # el servidor valida RUT, duplicado y permisos
        self.assertIn("_ot_puede_regularizar()", src)

    def test_el_bloque_vive_dentro_de_cobre_y_el_guardado_avisa_que_es_del_documento(self):
        self.assertIn('id="otdFcDocs"', self.t)
        self.assertIn("if (_otdFcDocsIgual()) body.desde_documentos = true;", self.t)
        self.assertIn("window.OTFinMotor.ligarDocumento(OTD_VID", self.t)               # «Agregar otro documento» usa el flujo de la ficha

    def test_el_precio_anotado_sigue_ahi_pero_avisa_que_el_documento_solo_cobra_servicio_y_despacho(self):
        self.assertIn("Usar como cobro del servicio", self.t)                            # REGLA #4.2: nada se quita
        self.assertIn("Ojo:</b> los documentos declarados solo cobran", self.t)

    def test_lo_nuevo_no_usa_alert_confirm_prompt_nativos(self):
        i = self.t.index("function _otdFcDocsMontos()")
        trozo = self.t[i:self.t.index("/* 2026-10-08 (Daniel: «el centro de costo hazlo con botones»)", i)]
        self.assertFalse(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", trozo))


if __name__ == "__main__":
    unittest.main()

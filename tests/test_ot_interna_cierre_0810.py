"""Dos problemas reales de producción del 08-10-2026 (Daniel).

1) OT INTERNA AL CERRAR. «La autorización del trabajo interno estaba presentando problemas, no deberían para el cierre
   ya que son trabajos internos y no tienen clientes ni facturas o documentos.» En la OT-2026-00284 (interna, sin
   cliente) alguien apretó «Pedir autorización» y el servidor respondió 400 NO_APLICA: la pantalla (el motor de
   finanzas, barra «Modificar») se la ofrecía aunque el servidor no la pide. Regla: una OT interna SIN cliente no
   muestra bloqueo de documento, factura, cobro ni autorización; el cierre dice «Trabajo interno: no necesita
   documento ni autorización».

2) ANEXO «ENVIADO» QUE NO SE ENVIÓ. La OT-2026-00287 creó su Anexo N° 212 y la ficha decía «Enviado a
   lyt.milling@gmail.com», pero el kill switch frenó el correo. Ahora el servidor distingue «llave apagada» de «fallo»,
   guarda que el correo NO salió (el anexo queda listo para firmar, con su candado) y la tarjeta ofrece copiar el
   enlace para WhatsApp o reenviar el correo. No se toca el kill switch ni nada que le llegue al cliente.

3) `_notificar_cierre_ot_async` (borrada en el commit f3cb145e) queda con guarda en sus dos llamadas.

Sin BD ni Flask: funciones de app.py extraídas con ast (mismo patrón que tests/test_ot_puerta_documento.py), la plantilla
del anexo renderizada con jinja2 y el motor JS ejecutado en node con un DOM mínimo.
Correr con:  py -m unittest tests.test_ot_interna_cierre_0810
"""
import ast
import json
import os
import re
import shutil
import subprocess
import tempfile
import types
import unittest
from datetime import datetime, timedelta

from tests import test_ot_puerta_documento as TPD
from tests.test_incidencias_bajas import _codigo_y_arbol, _fuente_de

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXT_INTERNA = "Trabajo interno: no necesita documento ni autorización"


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


def _extraer(amb, funcs=(), consts=()):
    """Ejecuta en `amb` las funciones y constantes (Assign) de app.py indicadas, sin decoradores."""
    _, arbol = _codigo_y_arbol()
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                and nodo.targets[0].id in consts:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in funcs:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in tuple(funcs) + tuple(consts) if n not in amb]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return amb


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) OT INTERNA
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _amb_recorrido():
    amb = TPD._cargar()   # la puerta y sus stubs de BD (docs_bd, auts_bd, contrato_real_bd)
    _extraer(amb,
             funcs=("_ot_fin_clp", "_ot_finanzas", "_ot_fin_rep_liviano", "_ot_interna_sin_cliente",
                    "ot_api_recorrido", "ot_api_panorama"),
             consts=("_OT_FIN_FUENTE_COBRO", "_OT_FIN_UMBRAL_BAJO", "_OT_TXT_INTERNA", "_OT2_CENTROS_COSTO",
                     "_OT_PANORAMA_MAX_LECTURAS_ERP"))
    amb["jsonify"] = lambda d: d
    amb["_ot_fin_cols_sql"] = lambda alias="v": "v.cliente_id"
    amb["_ot_repuestos_desglose"] = lambda ids: {}
    amb["_ot_docs_listar"] = lambda vid: {"documentos": []}
    amb["chile_fmt_filter"] = lambda d, f=None: "08/10/2026 16:24"
    amb["_ot_aut_json"] = lambda a: a
    amb["_es_rol_tecnico"] = lambda: False
    amb["_ot_panorama_docs"] = lambda vid, v: []
    amb["_ot_puede_finanzas_cierre"] = lambda *a, **k: True
    amb["_ot_puede_regularizar"] = lambda *a, **k: True
    amb["g"] = types.SimpleNamespace(user={})
    return amb


_FILA_BASE = {"id": 461, "numero_ot": "OT-2026-00284", "estado": "pendiente_aprobacion", "cerrada_at": None,
              "created_by": "aaron", "centro_costo": "sstt", "estado_facturacion": None, "factura_tido": None,
              "factura_nudo": None, "contrato_id": None, "cobro_cero_motivo": None,
              "cobro_cero_autorizacion_id": None, "cobro_cero_argumento": None, "garantia_motivo": None,
              "cliente": None, "tipo_cliente": None, "modalidad_cobro": "interno", "cubierto_por": "cliente",
              "tipo": "revision_interna", "cliente_id": None, "costo": None, "zz_monto": None, "zz_codigo": None,
              "zz_envio_monto": None, "valor_origen": None, "costo_proveedor": None, "costo_despacho": None,
              "proveedor_tipo": "interno", "valorizado_clp": None, "valorizado_fuente": None,
              "zz_motivo_manual": None, "contrato_real": 0}


def _recorrido(**fila):
    amb = _amb_recorrido()
    row = dict(_FILA_BASE, **fila)
    amb["mysql_fetchone"] = lambda *a, **k: dict(row)
    return amb["ot_api_recorrido"](461), amb


def _panorama(**fila):
    amb = _amb_recorrido()
    row = {"id": 461, "numero_ot": "OT-2026-00284", "estado": "pendiente_aprobacion", "centro_costo": "sstt",
           "cliente_id": None, "proveedor_tipo": "interno", "costo_proveedor": None, "costo_despacho": None,
           "zz_monto": None, "zz_envio_monto": None, "factura_asociada_por": None, "estado_facturacion": None,
           "cliente": None, "tecnico": "Jaizer"}
    row.update(fila)
    amb["mysql_fetchone"] = lambda *a, **k: dict(row)
    return amb["ot_api_panorama"](461)


class TestRecorridoInterna(unittest.TestCase):
    def test_interna_no_tiene_ningun_paso_de_documento_cobro_ni_cierre_en_falta(self):
        r, _ = _recorrido()
        self.assertTrue(r["interna"])
        self.assertFalse(r["puede_pedir_autorizacion"])
        por_n = {p["n"]: p for p in r["pasos"]}
        for n in (1, 2, 3):
            self.assertEqual(por_n[n]["estado"], "no_aplica", f"paso {n}")
        self.assertNotEqual(por_n[6]["estado"], "falta")
        self.assertEqual(por_n[6]["texto"], TXT_INTERNA)
        self.assertEqual(por_n[2]["texto"], TXT_INTERNA)
        self.assertTrue(r["puerta"]["ok"])
        self.assertEqual(r["puerta"]["via"], "interna_sin_cliente")

    def test_interna_con_la_forma_que_la_hacia_fallar(self):
        """Interna SIN cliente pero con modalidad/tipo de una OT de cliente (el recorrido la leía como cobrable o como
        garantía y marcaba «falta documento» / «$0 sin autorización»): la regla es SIN cliente."""
        for fila in ({"modalidad_cobro": "pagado", "tipo": "instalacion"},
                     {"modalidad_cobro": "garantia", "cubierto_por": "garantia", "tipo": "garantia"},
                     {"modalidad_cobro": "sin_costo", "cobro_cero_motivo": "regalia"}):
            r, _ = _recorrido(**fila)
            self.assertTrue(r["interna"], fila)
            self.assertEqual([p["n"] for p in r["pasos"] if p["estado"] == "falta" and p["n"] in (1, 2, 3, 6)], [], fila)
            self.assertEqual(r["pasos"][5]["texto"], TXT_INTERNA, fila)
            self.assertFalse(r["puede_pedir_autorizacion"], fila)

    def test_interna_cerrada_dice_que_no_necesito_documento(self):
        r, _ = _recorrido(estado="cerrada", cerrada_at=datetime(2026, 10, 8, 19, 24))
        p6 = r["pasos"][5]
        self.assertEqual(p6["estado"], "hecho")
        self.assertIn(TXT_INTERNA, p6["texto"])

    def test_una_ot_de_cliente_sin_documento_sigue_pidiendo_documento_y_autorizacion(self):
        """No se aflojó nada para las OT de cliente (REGLA #24)."""
        r, _ = _recorrido(cliente_id=7, cliente="LA DEHESA", modalidad_cobro="pagado", tipo="instalacion")
        self.assertFalse(r["interna"])
        self.assertTrue(r["puede_pedir_autorizacion"])
        por_n = {p["n"]: p for p in r["pasos"]}
        self.assertEqual(por_n[2]["estado"], "falta")
        self.assertEqual(por_n[6]["estado"], "falta")
        self.assertFalse(r["puerta"]["ok"])

    def test_el_costo_del_proveedor_interno_propio_no_falta(self):
        r, _ = _recorrido()
        self.assertEqual(r["pasos"][3]["estado"], "hecho")


class TestPanoramaInterna(unittest.TestCase):
    def test_marca_interna_y_el_tecnico_propio_cuesta_cero_no_falta(self):
        p = _panorama()
        self.assertTrue(p["interna"])
        tec = next(x for x in p["costos"]["registros"] if x["clave"] == "tecnico")
        self.assertFalse(tec["falta"], "un trabajo interno de técnico propio no tiene a quién pagarle")
        self.assertEqual(tec["monto"], 0.0)

    def test_proveedor_externo_en_interna_si_pide_declarar_su_costo(self):
        p = _panorama(proveedor_tipo="externo")
        tec = next(x for x in p["costos"]["registros"] if x["clave"] == "tecnico")
        self.assertTrue(tec["falta"])

    def test_ot_de_cliente_no_es_interna_y_conserva_el_falta(self):
        p = _panorama(cliente_id=7, cliente="LA DEHESA")
        self.assertFalse(p["interna"])
        tec = next(x for x in p["costos"]["registros"] if x["clave"] == "tecnico")
        self.assertTrue(tec["falta"])


class TestMismaDefinicionQueLaPuerta(unittest.TestCase):
    def test_el_helper_y_la_puerta_coinciden_en_toda_forma_de_cliente_vacio(self):
        amb = _amb_recorrido()
        for cid in (None, "", 0, "0"):
            self.assertTrue(amb["_ot_interna_sin_cliente"]({"cliente_id": cid}), cid)
            for momento in ("crear", "cerrar"):
                r = amb["_ot_puerta_documento"]({"cliente_id": cid, "tipo": "instalacion", "modalidad_cobro": "pagado",
                                                 "documentos": [], "autorizaciones": []}, momento)
                self.assertTrue(r["ok"], (cid, momento))
                self.assertEqual(r["via"], "interna_sin_cliente")
        for cid in (7, "7", 123):
            self.assertFalse(amb["_ot_interna_sin_cliente"]({"cliente_id": cid}), cid)
        # Una fila sin la clave (parcial) o vacía NO se toma por interna: no se le esconden botones a una OT de cliente.
        self.assertFalse(amb["_ot_interna_sin_cliente"](None))
        self.assertFalse(amb["_ot_interna_sin_cliente"]({}))
        self.assertFalse(amb["_ot_interna_sin_cliente"]({"tipo": "instalacion"}))

    def test_aprobar_cierre_exime_a_la_interna_de_todo_candado_de_documento_o_autorizacion(self):
        src = _fuente_de("mant_ot_aprobar_cierre")
        self.assertIn('_interna_cierre = _ot_es_interna(v) and not v.get("cliente_id")', src)
        # Cada candado de documento / cobro / saldo / proveedor es `... and not _interna_cierre`.
        self.assertGreaterEqual(src.count("not _interna_cierre"), 7)
        for codigo, ventana in (("_p_cierre = _ot_puerta_documento", 300), ('"SIN_FACTURA"', 900), ('"SIN_VALORIZAR"', 900),
                                ("_ot_saldo_chequear", 900), ('"SIN_COSTO_PROVEEDOR"', 600),
                                ('"ANEXO_DESACTUALIZADO"', 3500), ('"SOLO_NOTA_VENTA"', 900)):
            i = src.index(codigo)
            previo = src[max(0, i - ventana):i]
            self.assertIn("not _interna_cierre", previo, f"{codigo} no está protegido para la interna")

    def test_el_servidor_sigue_rechazando_pedir_autorizacion_para_una_interna(self):
        src = _fuente_de("ot_aut_api_crear")
        self.assertIn('"error_codigo": "NO_APLICA"', src)
        self.assertIn("Una OT interna sin cliente no necesita autorización", src)

    def test_la_plantilla_del_cierre_usa_la_misma_definicion(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("{% set _es_int = (not v.cliente_id) %}", det)


class TestPantallaMotorInterna(unittest.TestCase):
    """El motor de finanzas (el mismo en la ficha y en el modal de cierre) ejecutado en node con un DOM mínimo."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node no está instalado")
        cls.tmp = tempfile.mkdtemp(prefix="motor_interna_")
        cls.harness = os.path.join(cls.tmp, "harness.js")
        with open(cls.harness, "w", encoding="utf-8") as f:
            f.write(r"""
const fs = require('fs');
const datos = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
global.window = global;
const el = {
  innerHTML: '', classList: { add() {}, remove() {} },
  getAttribute(k) { return { 'data-vid': '461', 'data-modo': process.argv[4] || 'ficha' }[k] || null; },
  addEventListener() {}, closest() { return null; }, querySelector() { return null; }, contains() { return true; }
};
global.document = { readyState: 'complete', querySelectorAll() { return [el]; }, getElementById() { return null; },
                    createElement() { return { style: {}, setAttribute() {}, appendChild() {} }; }, addEventListener() {} };
global.ilusToast = function () {};
global.fetch = function (url) {
  const j = url.indexOf('/recorrido') >= 0 ? datos.recorrido : datos.panorama;
  return Promise.resolve({ ok: true, status: 200, json() { return Promise.resolve(j); } });
};
require(process.argv[2]);
setTimeout(function () { process.stdout.write(el.innerHTML); }, 80);
""")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _pintar(self, recorrido, panorama, modo="ficha"):
        datos = os.path.join(self.tmp, "datos.json")
        with open(datos, "w", encoding="utf-8") as f:
            json.dump({"recorrido": recorrido, "panorama": panorama}, f)
        r = subprocess.run([self.node, self.harness, os.path.join(RAIZ, "static", "ot_fin_motor.js"), datos, modo],
                           capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    @staticmethod
    def _datos(interna):
        pasos_int = [
            {"n": 1, "titulo": "¿Se cobra?", "estado": "no_aplica", "texto": "Trabajo interno: no se le cobra a nadie"},
            {"n": 2, "titulo": "Documentos", "estado": "no_aplica", "texto": TXT_INTERNA},
            {"n": 3, "titulo": "Cobré", "estado": "no_aplica", "texto": "Trabajo interno: no hay cobro que declarar"},
            {"n": 4, "titulo": "Me cobró el proveedor", "estado": "hecho", "texto": "Instalación $0"},
            {"n": 5, "titulo": "Margen", "estado": "hecho", "texto": "Trabajo interno. Nos costó $0."},
            {"n": 6, "titulo": "Revisión al cerrar", "estado": "pendiente", "texto": TXT_INTERNA}]
        pasos_cli = [
            {"n": 1, "titulo": "¿Se cobra?", "estado": "hecho", "texto": "Se le cobra al cliente"},
            {"n": 2, "titulo": "Documentos", "estado": "falta", "texto": "Sin documento validado del ERP"},
            {"n": 3, "titulo": "Cobré", "estado": "falta", "texto": "Falta lo que cobraste"},
            {"n": 4, "titulo": "Me cobró el proveedor", "estado": "falta", "texto": "Falta lo que te cobró el técnico"},
            {"n": 5, "titulo": "Margen", "estado": "falta", "texto": "Falta el documento de cobro."},
            {"n": 6, "titulo": "Revisión al cerrar", "estado": "falta", "texto": "Falta el documento"}]
        fin = {"cobra": not interna, "cobertura_txt": "Trabajo interno" if interna else "Se cobra", "clase": "info",
               "frase": "Trabajo interno. Nos costó $0.", "label": "x", "avisos": [],
               "cobre": {"servicio": 0, "despacho": 0, "total": 0, "hay": not interna},
               "me_cobraron": {"tecnico": 0, "despacho": 0, "repuestos": 0, "total": 0},
               "queda": {"total": 0, "pct": None, "mostrar": True}, "valorizado": {"monto": None}}
        rec = {"ok": True, "numero_ot": "OT-2026-00284", "interna": interna, "puede_pedir_autorizacion": not interna,
               "pasos": pasos_int if interna else pasos_cli, "fin": fin, "solicitud_pendiente": None, "autorizaciones": [],
               "cobro_cero": {}, "superadmin": False}
        pan = {"ok": True, "numero_ot": "OT-2026-00284", "cliente": "" if interna else "LA DEHESA", "interna": interna,
               "contadores": {"total": 0}, "documentos": [], "saldo": None,
               "costos": {"registros": [{"clave": "tecnico", "rotulo": "Instalación", "detalle": "propio", "monto": 0, "falta": False}],
                          "total": 0, "sin_costo": 0},
               "centro": {"valor": "sstt", "nombre": "Servicio Técnico", "opciones": [{"v": "sstt", "n": "Servicio Técnico"}]},
               "puede_editar": True, "puede_regularizar": True, "superadmin": False, "cerrada": False}
        return rec, pan

    PROHIBIDO = ("Pedir autorización", "Autorizar", "declarar $0", "Agregar factura", "Esperando autorización",
                 "pide la autorización", "Sin autorización de", "Declarar lo que cobré", "data-fm-act=\"pedirCero\"",
                 "data-fm-act=\"pedirCierre\"", "data-fm-act=\"ligarDoc\"", "data-fm-act=\"declararCobro\"",
                 "No tiene ningún documento", "no tiene ningún documento")

    def test_interna_no_muestra_ni_ofrece_documento_cobro_ni_autorizacion(self):
        for modo in ("ficha", "modal"):
            html = self._pintar(*self._datos(True), modo=modo)
            self.assertIn("Finanzas y documentos", html, modo)
            for malo in self.PROHIBIDO:
                self.assertNotIn(malo, html, f"{modo}: la OT interna ofrece «{malo}»")
            self.assertIn(TXT_INTERNA, html, modo)
            # Lo único que sigue pudiendo modificar es el costo del proveedor.
            self.assertIn('data-fm-act="corregirProv"', html, modo)

    def test_una_ot_de_cliente_sigue_ofreciendo_todo(self):
        html = self._pintar(*self._datos(False))
        for ok in ('data-fm-act="ligarDoc"', 'data-fm-act="pedirCero"', 'data-fm-act="pedirCierre"',
                   "Pedir autorización a Daniel", "No se cobra: declarar $0"):
            self.assertIn(ok, html, ok)

    def test_el_motor_bloquea_los_caminos_de_autorizacion_para_una_interna(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("function esInterna(inst)", js)
        for fn in ("function ligarDoc(inst, pre) {", "function pedirAutorizacion(inst, tipo) {", "function declararCobro(inst) {"):
            i = js.index(fn)
            self.assertIn("esInterna(inst)", js[i:i + 200], fn)
        self.assertIn("SOLO_CLIENTE[act] && esInterna(inst)", js)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) ANEXO CUYO CORREO NO SALIÓ
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _amb_correo(global_on=True, bloqueados=(), real=None):
    """_send_ilus_email real, con las llaves y el envío reales simulados."""
    amb = {"print": lambda *a, **k: None, "re": re, "json": json}
    _extraer(amb, funcs=("_email_marcar_bloqueo", "_email_ultimo_bloqueo", "_email_motivo_bloqueo_txt",
                         "_email_resultado", "_send_ilus_email", "_modulo_desde_evento"),
             consts=("_KS_MODULO_TXT",))
    amb["g"] = types.SimpleNamespace()
    amb["comm_is_enabled"] = lambda canal: global_on
    amb["_modulo_canal_bloqueado"] = lambda mod, canal: mod in bloqueados
    amb["_email_log"] = lambda *a, **k: 1
    amb["_send_ilus_email_real"] = real or (lambda to, subj, html, **kw: True)
    return amb


class TestCorreoBloqueadoSeDistingue(unittest.TestCase):
    def test_llave_del_modulo_apagada(self):
        amb = _amb_correo(bloqueados=("mantenciones",))
        ok = amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>", modulo="mantenciones")
        self.assertFalse(ok)
        r = amb["_email_resultado"](ok)
        self.assertFalse(r["salio"])
        self.assertEqual(r["causa"], "apagado")
        self.assertEqual(r["mensaje"], "El correo no salió: el envío de correos de Mantenciones está apagado")

    def test_el_anexo_sin_modulo_cae_en_general_y_lo_dice(self):
        """El correo del anexo no pasa `modulo`: lo clasifica como «general». La frase dice qué llave lo frenó."""
        amb = _amb_correo(bloqueados=("general",))
        ok = amb["_send_ilus_email"]("lyt.milling@gmail.com", "ILUS · Nueva orden de trabajo — firma el Anexo N° 212", "<p>x</p>")
        r = amb["_email_resultado"](ok)
        self.assertEqual(r["causa"], "apagado")
        self.assertEqual(r["modulo"], "general")
        self.assertIn("General", r["mensaje"])

    def test_interruptor_global_apagado(self):
        amb = _amb_correo(global_on=False)
        ok = amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>")
        r = amb["_email_resultado"](ok)
        self.assertEqual(r["causa"], "apagado")
        self.assertIn("interruptor general", r["mensaje"])

    def test_fallo_del_proveedor_de_correo_no_es_apagado(self):
        amb = _amb_correo(real=lambda *a, **k: False)
        amb["g"]._last_email_error = "SMTP 550 buzón no existe"
        ok = amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>")
        # _send_ilus_email limpia el error anterior antes de enviar: lo vuelve a poner el envío real.
        amb["g"]._last_email_error = "SMTP 550 buzón no existe"
        r = amb["_email_resultado"](ok)
        self.assertFalse(r["salio"])
        self.assertEqual(r["causa"], "fallo")
        self.assertTrue(r["mensaje"].startswith("El correo no salió: "))

    def test_salio(self):
        amb = _amb_correo()
        ok = amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>")
        self.assertTrue(ok)
        r = amb["_email_resultado"](ok)
        self.assertTrue(r["salio"])
        self.assertEqual(r["causa"], "salio")
        self.assertEqual(r["mensaje"], "")

    def test_el_bloqueo_anterior_no_contamina_el_siguiente_correo(self):
        bloq = {"mantenciones"}
        amb = _amb_correo()
        amb["_modulo_canal_bloqueado"] = lambda mod, canal: mod in bloq
        self.assertFalse(amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>", modulo="mantenciones"))
        bloq.clear()
        ok = amb["_send_ilus_email"]("a@b.cl", "x", "<p>x</p>", modulo="mantenciones")
        self.assertTrue(ok)
        self.assertIsNone(amb["_email_ultimo_bloqueo"]())

    def test_no_cambia_la_firma_ni_el_retorno_para_los_otros_llamadores(self):
        src = _fuente_de("_send_ilus_email")
        self.assertIn("-> bool:", src)
        self.assertEqual(len(re.findall(r"return False", src)), 2)   # las dos llaves, igual que antes
        _, arbol = _codigo_y_arbol()
        f = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "_send_ilus_email")
        self.assertEqual([a.arg for a in f.args.args], ["to_addr", "subject", "html_body"])


# ── El endpoint de envío manual con un correo bloqueado ──────────────────────────────────────────────────────
class _Req:
    def __init__(self, cuerpo):
        self._c = cuerpo

    def get_json(self, silent=True):
        return self._c


class _Obj(dict):
    """Fila que se lee como dict y por atributo (como las de pymysql en este proyecto)."""
    def __getattr__(self, k):
        return self[k]


def _amb_enviar(correo_ok, bloqueados=(), anexo=None, cuerpo=None):
    amb = _amb_correo(bloqueados=bloqueados, real=lambda *a, **k: correo_ok)
    _extraer(amb, funcs=("ot2_api_anexo_enviar", "_anexo_registrar_correo"))
    import secrets as _secrets
    ahora = datetime.utcnow()
    fila = {"id": 54, "numero": 212, "ot_id": 464, "estado": "borrador", "token": None, "token_expira_at": None,
            "enviado_at": None, "proveedor_nombre": "LOGISTICA Y TRANSPORTES MILLING SPA"}
    fila.update(anexo or {})
    sql = []
    amb.update({
        "datetime": datetime, "timedelta": timedelta, "secrets": _secrets, "re": re,
        "request": _Req(cuerpo if cuerpo is not None else {"email": "lyt.milling@gmail.com", "telefono": ""}),
        "jsonify": lambda d: d, "_es_rol_tecnico": lambda: False,
        "mysql_fetchone": lambda q, p=(): (dict(fila) if "FROM mant_anexos" in q else {"numero_ot": "OT-2026-00287", "titulo": "Instalación", "razon_social": "Cliente"}),
        "mysql_execute": lambda q, p=(): sql.append((q, p)),
        "current_username": lambda: "daniel", "_mant_log": lambda *a, **k: sql.append(("LOG", a)),
        "url_for": lambda *a, **k: "https://ilus.test/firmar-anexo/" + k.get("token", "x"),
        "_ot2_err": lambda m, c, http=400: ({"ok": False, "error": m}, http),
        "_render_comm_template": lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sin plantilla")),
        "_brand_subject": lambda s: "ILUS · " + s, "_anexo_archivar_en_ot": lambda *a, **k: None,
        "_anexo_dict": lambda a: a, "_comm_render_email_document": lambda *a, **k: "<p>x</p>",
        "PDFEngineUnavailable": type("PDFEngineUnavailable", (Exception,), {}),
    })
    amb["_sql"] = sql
    return amb


class TestAnexoEnviarDiceLaVerdad(unittest.TestCase):
    def test_con_el_correo_bloqueado_el_anexo_no_queda_enviado_a_secas(self):
        amb = _amb_enviar(correo_ok=False, bloqueados=("general",))
        r = amb["ot2_api_anexo_enviar"](54)
        self.assertTrue(r["ok"], "el anexo sí quedó listo para firmar (link generado)")
        self.assertTrue(r["link"])
        self.assertFalse(r["correo_enviado"])
        self.assertTrue(r["correo_intentado"])
        self.assertEqual(r["correo_causa"], "apagado")
        self.assertTrue(r["correo_mensaje"].startswith("El correo no salió: el envío de correos de "))
        self.assertTrue(r["correo_mensaje"].endswith("está apagado"))
        # Quedó guardado que NO salió, con el motivo.
        upd = [p for q, p in amb["_sql"] if isinstance(q, str) and "correo_estado" in q]
        self.assertEqual(len(upd), 1)
        self.assertEqual(upd[0][0], "no_salio")
        self.assertIn("apagado", upd[0][1])
        # Y la bitácora de la OT lo dice.
        logs = [a for q, a in amb["_sql"] if q == "LOG" and a[2] == "anexo_correo_no_salio"]
        self.assertEqual(len(logs), 1)
        self.assertIn("El correo no salió", logs[0][3])

    def test_reenvio_manual_aplica_igual(self):
        amb = _amb_enviar(correo_ok=False, bloqueados=("general",),
                          anexo={"estado": "enviado", "enviado_at": datetime.utcnow(), "token": "tokviejo1234",
                                 "token_expira_at": datetime.utcnow() + timedelta(days=5)})
        r = amb["ot2_api_anexo_enviar"](54)
        self.assertFalse(r["correo_enviado"])
        self.assertIn("no salió", r["correo_mensaje"])
        self.assertTrue(any("anexo_correo_no_salio" in str(a) for q, a in amb["_sql"] if q == "LOG"))

    def test_si_el_correo_sale_se_guarda_que_salio(self):
        amb = _amb_enviar(correo_ok=True)
        r = amb["ot2_api_anexo_enviar"](54)
        self.assertTrue(r["correo_enviado"])
        self.assertEqual(r["correo_mensaje"], "")
        upd = [p for q, p in amb["_sql"] if isinstance(q, str) and "correo_estado" in q]
        self.assertEqual(upd[0][0], "salio")
        self.assertIsNone(upd[0][1])

    def test_solo_el_link_o_whatsapp_no_toca_el_estado_del_correo(self):
        amb = _amb_enviar(correo_ok=False, bloqueados=("general",), cuerpo={"email": "", "telefono": "+56911112222"})
        r = amb["ot2_api_anexo_enviar"](54)
        self.assertTrue(r["ok"])
        self.assertFalse(r["correo_intentado"])
        self.assertEqual(r["correo_mensaje"], "")
        self.assertFalse([1 for q, p in amb["_sql"] if isinstance(q, str) and "correo_estado" in q])
        self.assertTrue(r["whatsapp_link"])

    def test_el_correo_del_anexo_va_al_proveedor_no_al_cliente(self):
        src = _fuente_de("ot2_api_anexo_enviar")
        self.assertEqual(len(re.findall(r"_send_ilus_email\(", src)), 2)   # plantilla + HTML de siempre
        self.assertNotIn("contacto_email", src)
        self.assertNotIn("_mant_ot_email_cliente", src)


class TestCaminoAutomaticoDelAnexo(unittest.TestCase):
    """El anexo que crea _ot2_crear_core (también al aprobar una autorización 'crear_sin_documento')."""

    def test_usa_el_resultado_real_del_correo(self):
        src = _fuente_de("_ot2_crear_core")
        i = src.index("_env_auto = _send_ilus_email(")
        trozo = src[i - 200:i + 7000]
        self.assertIn("_res_auto = _email_resultado(_env_auto)", trozo)
        self.assertIn("_anexo_registrar_correo(_aid_auto, _res_auto)", trozo)
        self.assertIn("NO salió", trozo)
        self.assertIn("Copia el enlace para mandarlo por WhatsApp o reenvía el correo", trozo)
        # La bitácora de la OT ya no dice «sin correo del proveedor» cuando el correo SÍ existía y fue frenado.
        self.assertIn("(_prov_email and _res_auto)", trozo)

    def test_el_estado_del_anexo_sigue_siendo_enviado_listo_para_firmar(self):
        """Candado de la OT: se conserva (el enlace es válido y se puede mandar por otro canal)."""
        src = _fuente_de("_ot2_crear_core")
        i = src.index("_correo_ok = False")
        self.assertIn("estado='enviado'", src[i:i + 900])

    def test_la_aprobacion_devuelve_los_avisos_de_la_creacion(self):
        self.assertIn('"avisos": cuerpo.get("avisos") or []', _fuente_de("ot_aut_api_aprobar"))


class TestAnexoAnteriorSeDeduceDelHistorial(unittest.TestCase):
    def _amb(self, filas):
        amb = {"print": lambda *a, **k: None, "re": re}
        _extraer(amb, funcs=("_anexo_correo_desde_registro", "_email_motivo_bloqueo_txt"), consts=("_KS_MODULO_TXT",))
        amb["mysql_fetchall"] = lambda *a, **k: filas
        return amb

    A = {"enviado_email": "lyt.milling@gmail.com", "enviado_at": datetime(2026, 10, 8, 19, 24), "numero": 212}

    def test_bloqueado_por_la_llave_del_modulo(self):
        r = self._amb([{"estado": "bloqueado", "error_msg": 'Llave de paso cerrada para módulo "general" — email no enviado'}])["_anexo_correo_desde_registro"](self.A)
        self.assertEqual(r["estado"], "no_salio")
        self.assertEqual(r["motivo"], "el envío de correos de General está apagado")

    def test_bloqueado_por_el_interruptor_global(self):
        r = self._amb([{"estado": "bloqueado", "error_msg": "Email deshabilitado por superadmin (kill switch global ON)"}])["_anexo_correo_desde_registro"](self.A)
        self.assertEqual(r["estado"], "no_salio")
        self.assertIn("interruptor general", r["motivo"])

    def test_fallido(self):
        r = self._amb([{"estado": "fallido", "error_msg": "x"}])["_anexo_correo_desde_registro"](self.A)
        self.assertEqual(r["estado"], "no_salio")

    def test_si_alguno_salio_no_se_alarma(self):
        r = self._amb([{"estado": "bloqueado", "error_msg": ""}, {"estado": "enviado", "error_msg": ""}])["_anexo_correo_desde_registro"](self.A)
        self.assertEqual(r["estado"], "salio")

    def test_sin_registro_no_inventa_nada(self):
        self.assertIsNone(self._amb([])["_anexo_correo_desde_registro"](self.A))
        self.assertIsNone(self._amb([{"estado": "bloqueado", "error_msg": ""}])["_anexo_correo_desde_registro"](dict(self.A, enviado_email="")))

    def test_la_ficha_lo_usa_solo_si_nadie_abrio_ni_firmo(self):
        src = _fuente_de("ot2_detalle")
        i = src.index("_anexo_correo_desde_registro(anexo)")
        previo = src[i - 500:i]
        for cond in ('not anexo.get("correo_estado")', 'anexo.get("enviado_at")', 'not anexo.get("visto_at")',
                     'not anexo.get("firmado_at")'):
            self.assertIn(cond, previo)


class TestEnlaceDelAnexo(unittest.TestCase):
    def _llamar(self, fila, tecnico=False):
        amb = {"print": lambda *a, **k: None}
        _extraer(amb, funcs=("ot2_api_anexo_enlace",))
        amb.update({"datetime": datetime, "jsonify": lambda d: d,
                    "_es_rol_tecnico": lambda: tecnico, "mysql_fetchone": lambda *a, **k: fila,
                    "url_for": lambda *a, **k: "https://ilus.test/firmar-anexo/" + k["token"]})
        res = amb["ot2_api_anexo_enlace"](54)
        return res if isinstance(res, tuple) else (res, 200)

    def test_enlace_vigente(self):
        d, _ = self._llamar({"id": 54, "numero": 212, "estado": "enviado", "token": "abc123",
                             "token_expira_at": datetime.utcnow() + timedelta(days=3), "enviado_tel": ""})
        self.assertTrue(d["ok"])
        self.assertEqual(d["link"], "https://ilus.test/firmar-anexo/abc123")
        self.assertIn("Anexo de Servicios N° 212", d["mensaje"])
        self.assertIn(d["link"], d["mensaje"])
        self.assertIn("\n", d["mensaje"])

    def test_enlace_vencido_dice_que_reenvie(self):
        d, http = self._llamar({"id": 54, "numero": 212, "estado": "enviado", "token": "abc123",
                                "token_expira_at": datetime.utcnow() - timedelta(days=1), "enviado_tel": ""})
        self.assertEqual(http, 409)
        self.assertEqual(d["error_codigo"], "ENLACE_NO_VIGENTE")

    def test_anexo_firmado_no_necesita_enlace(self):
        d, http = self._llamar({"id": 54, "numero": 212, "estado": "firmado", "token": "abc",
                                "token_expira_at": None, "enviado_tel": ""})
        self.assertEqual(http, 409)
        self.assertEqual(d["error_codigo"], "ANEXO_CERRADO")

    def test_tecnico_no_lo_ve(self):
        d, http = self._llamar({"id": 54}, tecnico=True)
        self.assertEqual(http, 403)


class TestTarjetaDelAnexoRenderizada(unittest.TestCase):
    """La tarjeta real de detalle.html, renderizada con jinja2 y los dos estados de un anexo."""

    @classmethod
    def setUpClass(cls):
        from jinja2 import Environment
        det = _leer("templates/ot2/detalle.html")
        i = det.index("{# ══ ANEXO DE SERVICIOS")
        j = det.index("{# ══ FIRMAR Y CERRAR OT")
        env = Environment()
        env.filters["chile_fmt"] = lambda d, f="%d/%m/%Y %H:%M": d.strftime(f) if d else ""
        env.filters["rut_fmt"] = lambda r: r
        cls.tpl = env.from_string(det[i:j])

    def _render(self, **anexo):
        base = {"id": 54, "numero": 212, "estado": "enviado", "proveedor_nombre": "LOGISTICA Y TRANSPORTES MILLING SPA",
                "proveedor_rut": "76.1-1", "firmante_nombre": None, "firmante_rut": None, "firmado_at": None,
                "enviado_at": datetime(2026, 10, 8, 19, 24), "visto_at": None, "enviado_email": "lyt.milling@gmail.com",
                "enviado_tel": "", "envios_n": 1, "documento_hash_cliente": None}
        base.update(anexo)
        return self.tpl.render(anexo=base, es_tecnico=False, v={"id": 464, "numero_ot": "OT-2026-00287"})

    def test_correo_no_salio(self):
        html = self._render(correo_estado="no_salio", correo_motivo="el envío de correos de General está apagado")
        self.assertIn("Listo para firmar — el correo no salió", html)
        self.assertIn("El correo no salió: el envío de correos de General está apagado", html)
        self.assertIn("Copiar enlace para enviar por WhatsApp", html)
        self.assertIn("Reenviar por correo", html)
        self.assertIn("otdAnexoCopiarEnlace(54)", html)
        self.assertIn("otdAnexoReenviarCorreo(this, 54", html)
        self.assertIn("NO salió", html)
        # Ya no cuenta la historia falsa.
        self.assertNotIn("Enviado — sin abrir todavía", html)
        self.assertNotIn("Todavía no abre el link", html)
        self.assertNotIn("Enviado a <b>", html)
        # El candado de la OT se sigue diciendo (el anexo está listo para firmar).
        self.assertIn("La OT está bloqueada para el técnico", html)

    def test_correo_salio_queda_como_siempre(self):
        html = self._render(correo_estado="salio")
        self.assertIn("Enviado — sin abrir todavía", html)
        self.assertIn("Todavía no abre el link", html)
        self.assertNotIn("Copiar enlace para enviar por WhatsApp", html)
        self.assertNotIn("el correo no salió", html)

    def test_anexo_anterior_sin_dato_queda_como_siempre(self):
        html = self._render()
        self.assertIn("Enviado — sin abrir todavía", html)
        self.assertNotIn("Copiar enlace para enviar por WhatsApp", html)

    def test_si_el_proveedor_ya_lo_abrio_no_se_alarma_aunque_el_correo_fallara(self):
        html = self._render(correo_estado="no_salio", correo_motivo="x", visto_at=datetime(2026, 10, 8, 20, 0), estado="visto")
        self.assertIn("Lo abrió y aún no firma", html)
        self.assertNotIn("Copiar enlace para enviar por WhatsApp", html)


class TestPantallaDelAnexoYDDL(unittest.TestCase):
    def test_modal_del_anexo_no_canta_enviado_cuando_el_correo_no_salio(self):
        modal = _leer("templates/ot2/_modal_anexo.html")
        self.assertIn("j.correo_intentado", modal)
        self.assertIn("j.correo_mensaje", modal)
        # El mensaje viejo solo queda para cuando NO se pidió correo (link / WhatsApp).
        i = modal.index("j.correo_intentado")
        self.assertLess(i, modal.index("Anexo enviado — comparte el link tú mismo"))

    def test_funciones_de_la_ficha(self):
        det = _leer("templates/ot2/detalle.html")
        self.assertIn("window.otdAnexoCopiarEnlace = async function(aid)", det)
        self.assertIn("window.otdAnexoReenviarCorreo = async function(btn, aid, email, tel, numero)", det)
        self.assertIn("'/ot/api/anexos/' + aid + '/enlace'", det)
        self.assertIn("'/ot/api/anexos/' + aid + '/enviar'", det)

    def test_columnas_nuevas_van_con_una_clausula_por_sentencia_regla_18(self):
        src = _fuente_de("_ensure_mant_anexos")
        for col in ("correo_estado", "correo_motivo", "correo_at"):
            m = re.search(r'\("%s",\s*"(ALTER TABLE mant_anexos ADD COLUMN %s[^"]*"(?:\s*"[^"]*")*)\)' % (col, col), src)
            self.assertIsNotNone(m, col)
            self.assertEqual(m.group(1).count("ADD COLUMN"), 1, col)
        # Y se verifican con information_schema antes del ALTER (barato si ya está aplicada).
        self.assertIn("SELECT COLUMN_NAME FROM information_schema.COLUMNS", src)

    def test_la_ficha_tolera_que_las_columnas_aun_no_existan(self):
        src = _fuente_de("ot2_detalle")
        self.assertIn("anexo sin columnas del correo", src)

    def test_sin_dialogos_nativos_en_lo_nuevo(self):
        patron = re.compile(r"(?<![\w.$])(?:window\.)?(alert|confirm|prompt)\s*\(")
        det = _leer("templates/ot2/detalle.html")
        i = det.index("window.otdAnexoCopiarEnlace")
        j = det.index("NAVEGACIÓN Y BÚSQUEDA DE EQUIPOS")
        self.assertIsNone(patron.search(det[i:j]))
        js = re.sub(r"/\*.*?\*/", "", _leer("static/ot_fin_motor.js"), flags=re.S)
        self.assertIsNone(patron.search(js))


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) _notificar_cierre_ot_async
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestNotificarCierreConGuarda(unittest.TestCase):
    def test_ninguna_llamada_directa_a_la_funcion_borrada(self):
        codigo, arbol = _codigo_y_arbol()
        definida = any(isinstance(n, ast.FunctionDef) and n.name == "_notificar_cierre_ot_async" for n in ast.walk(arbol))
        self.assertFalse(definida, "si la función vuelve a existir, esta guarda ya no hace falta")
        directas = re.findall(r"(?<![\w\"'])_notificar_cierre_ot_async\(", codigo)
        self.assertEqual(directas, [], "quedó una llamada sin guarda: NameError en cada cierre")
        self.assertEqual(codigo.count('globals().get("_notificar_cierre_ot_async")'), 2)

    def test_las_guardas_estan_fechadas_y_no_inventan_un_correo(self):
        for nombre in ("mant_ot_aprobar_cierre", "mant_ot_rechazar_cierre"):
            try:
                src = _fuente_de(nombre)
            except Exception:
                continue
            if "_notificar_cierre_ot_async" in src:
                self.assertIn("2026-10-08", src)
                self.assertNotIn("_send_ilus_email", src[src.index("_notificar_cierre_ot_async") - 400:src.index("_notificar_cierre_ot_async") + 400])


if __name__ == "__main__":
    unittest.main()

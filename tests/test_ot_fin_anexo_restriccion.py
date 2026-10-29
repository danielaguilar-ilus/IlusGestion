"""«Corregir finanzas» NO puede dejar lo que se le paga al proveedor distinto de su anexo — 2026-10-29 (REGLA #56 / #34).

Daniel: «si se modifica lo que me cobraron, ahí tenemos que modificar el anexo, y es peligroso: que tengamos restricciones. Yo sé que puedo hacer de
todo, pero a veces puedo pasar a llevar un anexo». Un anexo firmado es evidencia; el costo de la OT y el anexo son UN solo valor. Antes de esto el
superadmin podía cambiar el costo a otro monto sin que nada lo frenara; ahora el servidor lo rechaza (409 COSTO_ES_DEL_ANEXO) y el modal bloquea los
dos casilleros y ofrece «Igualar la OT al anexo».

Se prueba con datos de mentira: la regla pura, la ruta real con una base de mentira y las funciones del modal ejecutadas en Node.
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

import ot_anexo_costo as oac
from tests import _fuente_app as FA

PLANTILLA = os.path.join(FA.RAIZ, "templates", "ot2", "detalle.html")
ITEMS = [{"concepto": "Instalación equipos", "monto": 195000}, {"concepto": "Despacho", "monto": 195000}]


def _anexo(firmado=True, total=390000):
    return {"numero": 221, "estado": "firmado" if firmado else "enviado", "firmado": firmado, "total": total, "items": ITEMS, "compartido": False}


class TestReglaPura(unittest.TestCase):
    def setUp(self):
        self.f = FA.cargar(funciones=["_ot_fin_anexo_restriccion"], extra={"_oac": oac})["_ot_fin_anexo_restriccion"]

    def test_sin_anexo_no_hay_restriccion(self):
        self.assertIsNone(self.f(None, 100000, 0))

    def test_si_la_suma_es_la_del_anexo_se_puede(self):
        self.assertIsNone(self.f(_anexo(), 195000, 195000))
        self.assertIsNone(self.f(_anexo(), 390000, 0))                       # lo que importa es la suma
        self.assertIsNone(self.f(_anexo(), 389999.5, 0.0))                   # misma holgura que el cierre (1 peso)

    def test_un_anexo_firmado_pide_sustituirlo(self):
        r = self.f(_anexo(), 100000, 195000)
        self.assertEqual((r["numero"], r["total"], r["costo"], r["firmado"]), (221, 390000, 295000, True))
        self.assertIn("UN solo valor", r["texto"])
        self.assertIn("no se puede guardar", r["texto"])
        self.assertIn("SUSTITUIR el anexo", r["texto"])
        self.assertIn("Igualar la OT al anexo", r["texto"])

    def test_un_anexo_sin_firmar_pide_corregir_el_monto_en_el_anexo(self):
        r = self.f(_anexo(firmado=False), 100000, 195000)
        self.assertFalse(r["firmado"])
        self.assertIn("Corrige el monto en el anexo", r["texto"])
        self.assertNotIn("SUSTITUIR", r["texto"])

    def test_dejar_el_costo_sin_declarar_tambien_se_frena(self):
        r = self.f(_anexo(), None, None)
        self.assertIsNone(r["costo"])
        self.assertIn("sin declarar", r["texto"])


class TestAnexoVivo(unittest.TestCase):
    def llamar(self, vivo):
        f = FA.cargar(funciones=["_ot_fin_anexo_vivo"], extra={"_ot_anexo_vivo": lambda vid: vivo})["_ot_fin_anexo_vivo"]
        return f(151)

    def test_cuenta_todo_anexo_vivo_y_dice_si_esta_firmado(self):
        base = {"numero": 221, "total": 390000, "items": ITEMS, "compartido": False}
        self.assertTrue(self.llamar(dict(base, estado="firmado"))["firmado"])
        for est in ("borrador", "enviado", "visto", "vencido"):
            r = self.llamar(dict(base, estado=est))
            self.assertFalse(r["firmado"], est)
            self.assertEqual(r["total"], 390000)
        self.assertIsNone(self.llamar(None))

    def test_si_la_base_falla_no_rompe(self):
        def rota(vid):
            raise RuntimeError("x")
        f = FA.cargar(funciones=["_ot_fin_anexo_vivo"], extra={"_ot_anexo_vivo": rota})["_ot_fin_anexo_vivo"]
        self.assertIsNone(f(1))


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


class TestRutaDeCorreccion(unittest.TestCase):
    FILA = {"id": 151, "numero_ot": "OT-2026-00221", "estado": "cerrada", "tipo": "instalacion", "cliente_id": 5, "zz_monto": 600000,
            "zz_envio_monto": None, "costo_proveedor": 195000, "costo_despacho": 195000, "centro_costo": "sstt", "modalidad_cobro": "pagado",
            "cubierto_por": None, "costo": None, "zz_codigo": "ZZINSTALACION", "valor_origen": "zz", "proveedor_tipo": "externo",
            "valorizado_clp": None, "valorizado_fuente": None, "zz_motivo_manual": None, "contrato_real": 0, "ot_madre_id": None,
            "ot_relacion": None, "madre_valida": 0}

    def correr(self, payload, anexo, leidos=None):
        self.sql, self.logs = [], []
        m = self

        class _Saldo:
            _entero = staticmethod(lambda x: int(x or 0))

        def vivo(vid):
            if leidos is not None:
                leidos.append(vid)
            return anexo

        extra = {
            "json": json, "_oac": oac,
            "_ot_fin_es_superadmin": lambda: True, "mysql_fetchone": lambda sql, params=None: dict(self.FILA) if "mant_visitas" in sql else {"rut": "1-9"},
            "_OT_FIN_SELECT": "SELECT ... FROM mant_visitas v WHERE v.id=%s",
            "request": type("R", (), {"get_json": staticmethod(lambda silent=True: payload)})(),
            "_ot2_err": lambda msg, cod=None, status=400: ({"ok": False, "error": msg, "codigo": cod}, status),
            "jsonify": lambda d: d, "current_username": lambda: "daniel@sphs.cl", "_mant_log": lambda *a, **k: m.logs.append(a),
            "get_mysql": lambda: _Conn(m), "_ot_fin_rep_de": lambda vid: None, "_ot_es_interna": lambda v: False,
            "_ot_saldo_reanotar_aportes": lambda vid: None, "_ot_saldo_docs_de_ot": lambda vid: [],
            "_ot_saldo_chequear": lambda *a, **k: None, "_ot_saldo_autorizado_hasta": lambda **k: {"servicio": 0, "despacho": 0},
            "_saldo": _Saldo(), "_ot_fin_docs_cobro": lambda vid, cliente_id=None: None, "chile_fmt_filter": lambda *a, **k: "",
            "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico")], "_ot_fin_anexo_vivo": vivo,
        }
        amb = FA.cargar(
            funciones=["ot2_api_finanzas_corregir", "_ot_fin_corr_desde_docs", "_ot_finanzas", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp",
                       "_ot_fin_ganancia", "_ot_fin_anexo_restriccion"],
            constantes=["_OT_FIN_CORR_CAMPOS", "_OT_FIN_CORR_ROT", "_OT_FIN_CORR_MOTIVO_MIN", "_OT_FIN_ZZ_NO_SERVICIO", "_OT2_LINEA_ZZ",
                        "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_COBERTURA_TXT",
                        "_OT_FIN_UMBRAL_BAJO"], extra=extra)
        return amb["ot2_api_finanzas_corregir"](151)

    def cuerpo(self, **kw):
        base = {"motivo": "El proveedor cobró distinto a lo declarado al cerrar la OT.", "zz_monto": 600000, "costo_proveedor": 195000,
                "costo_despacho": 195000, "centro_costo": "sstt"}
        base.update(kw)
        return base

    def escribio(self):
        return any(("UPDATE mant_visitas" in s) or ("INSERT INTO mant_ot_finanzas_correcciones" in s) for s, _ in self.sql)

    def test_cambiar_el_costo_a_otro_monto_con_un_anexo_firmado_se_rechaza_y_no_escribe_nada(self):
        r, http = self.correr(self.cuerpo(costo_proveedor=100000), _anexo())
        self.assertEqual(http, 409)
        self.assertEqual(r["error_codigo"], "COSTO_ES_DEL_ANEXO")
        self.assertIn("SUSTITUIR el anexo", r["error"])
        self.assertEqual(r["anexo"], {"numero": 221, "total": 390000, "firmado": True})
        self.assertEqual(r["acciones"], ["sustituir_anexo", "igualar_al_anexo"])
        self.assertFalse(self.escribio(), "no se escribe ni la corrección ni el registro")

    def test_con_un_anexo_sin_firmar_la_salida_es_corregir_el_monto_en_el_anexo(self):
        r, http = self.correr(self.cuerpo(costo_despacho=1), _anexo(firmado=False))
        self.assertEqual((http, r["acciones"]), (409, ["editar_anexo", "igualar_al_anexo"]))
        self.assertFalse(self.escribio())

    def test_vaciar_el_costo_tambien_se_rechaza(self):
        r, http = self.correr(self.cuerpo(costo_proveedor=None), _anexo())
        self.assertEqual(http, 409)
        self.assertFalse(self.escribio())

    def test_igualar_la_ot_al_anexo_siempre_se_puede(self):
        r = self.correr(self.cuerpo(costo_proveedor=300000, costo_despacho=90000), _anexo())
        self.assertTrue(r["ok"], r)
        self.assertTrue(self.escribio())

    def test_si_el_costo_no_cambia_no_se_mira_el_anexo_aunque_la_ot_ya_difiera(self):
        leidos = []
        r = self.correr(self.cuerpo(zz_monto=700000), _anexo(total=999999), leidos)      # solo cambia el cobro al cliente
        self.assertTrue(r["ok"], r)
        self.assertEqual(leidos, [])

    def test_sin_anexo_vivo_el_costo_se_corrige_como_siempre(self):
        r = self.correr(self.cuerpo(costo_proveedor=100000), None)
        self.assertTrue(r["ok"], r)

    def test_la_correccion_ya_no_deja_avisos_posteriores_por_anexo(self):
        src = FA.fuente_funcion("ot2_api_finanzas_corregir")
        self.assertNotIn("aviso_anexo", src)                                              # el aviso era para cuando se dejaba pasar: ahora se frena antes
        self.assertIn("_ot_fin_anexo_restriccion(_ot_fin_anexo_vivo(vid)", src)
        self.assertLess(src.index("COSTO_ES_DEL_ANEXO"), src.index("UPDATE mant_visitas SET"))   # el candado va ANTES de escribir


class TestCorreccionesParaElModal(unittest.TestCase):
    def test_el_modal_recibe_el_anexo_vivo_y_con_que_igualar(self):
        src = FA.fuente_funcion("ot2_api_finanzas_correcciones")
        self.assertIn('"anexo": _anx_info', src)
        for k in ('"firmado": _anx_vivo["firmado"]', '"alineado"', '"igualar": _igualar'):
            self.assertIn(k, src)
        d = " ".join(FA.decoradores("ot2_api_finanzas_correcciones"))
        self.assertIn("_no_tecnico", d)                                                   # montos: nunca para técnicos (REGLAS #19 y #26)


def _fuente_modal():
    with open(PLANTILLA, encoding="utf-8") as fh:
        t = fh.read()
    i = t.index("var _otdFcAnexo = null;")
    j = t.index("var _OTD_FC_CAMPOS")
    return t[i:j]


@unittest.skipUnless(shutil.which("node"), "Node no está disponible")
class TestModalEnNode(unittest.TestCase):
    HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const els = {};
const el = id => els[id] = els[id] || {id, value: '', readOnly: false, title: '', hidden: true, innerHTML: '', className: '', focus(){}};
els.otdFc_costo_proveedor = Object.assign(el('otdFc_costo_proveedor'), {value: '100000'});
els.otdFc_costo_despacho = Object.assign(el('otdFc_costo_despacho'), {value: '195000'});
el('otdFcAnexoNota'); el('otdFc_motivo');
let recalculos = 0;
const f = new Function('_otdFcNum', '_otdFcEsc', '_otdFcPlata', 'document', 'otdFinCorrCalc', src +
  '; return {set(a){ _otdFcAnexo = a; }, dif: _otdFcAnexoDifiere, pintar: otdFinCorrAnexo, igualar: otdFinCorrIgualarAnexo};');
const num = id => { const v = els[id].value; return v === '' ? null : Number(v); };
const api = f(num, s => String(s), n => '$' + n, {getElementById: id => els[id]}, () => { recalculos++; api.pintar(); });
const out = {};
api.set(null); api.pintar();
out.sinAnexo = {oculto: els.otdFcAnexoNota.hidden, bloqueado: els.otdFc_costo_proveedor.readOnly};
api.set({numero: 221, total: 390000, firmado: true, igualar: {prov: 195000, desp: 195000}}); api.pintar();
out.distinto = {oculto: els.otdFcAnexoNota.hidden, clase: els.otdFcAnexoNota.className, html: els.otdFcAnexoNota.innerHTML,
                bloqueado: [els.otdFc_costo_proveedor.readOnly, els.otdFc_costo_despacho.readOnly], titulo: els.otdFc_costo_proveedor.title};
api.igualar();
out.igualado = {prov: els.otdFc_costo_proveedor.value, desp: els.otdFc_costo_despacho.value, motivo: els.otdFc_motivo.value,
                clase: els.otdFcAnexoNota.className, html: els.otdFcAnexoNota.innerHTML, recalculos, dif: api.dif()};
api.set({numero: 222, total: 50000, firmado: false, igualar: null}); els.otdFc_costo_proveedor.value = ''; api.pintar();
out.sinFirmar = {html: els.otdFcAnexoNota.innerHTML};
console.log(JSON.stringify(out));
"""

    def correr(self):
        with tempfile.TemporaryDirectory() as d:
            s, h = os.path.join(d, "m.js"), os.path.join(d, "h.js")
            with open(s, "w", encoding="utf-8") as fh:
                fh.write(_fuente_modal())
            with open(h, "w", encoding="utf-8") as fh:
                fh.write(self.HARNESS)
            r = subprocess.run([shutil.which("node"), h, s], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr)
            return json.loads(r.stdout.strip().splitlines()[-1])

    def test_el_modal_bloquea_los_casilleros_y_ofrece_igualar_al_anexo(self):
        o = self.correr()
        self.assertTrue(o["sinAnexo"]["oculto"])
        self.assertFalse(o["sinAnexo"]["bloqueado"])                                   # sin anexo, el costo se corrige como siempre
        d = o["distinto"]
        self.assertFalse(d["oculto"])
        self.assertEqual(d["bloqueado"], [True, True])
        self.assertIn("Anexo N° 221", d["titulo"])
        self.assertIn("sustituir el anexo", d["html"])                                  # firmado: la salida es sustituirlo
        self.assertIn("Igualar la OT al anexo", d["html"])
        self.assertIn("nota", d["clase"])                                               # ámbar: alertar sin gritar
        g = o["igualado"]
        self.assertEqual((g["prov"], g["desp"]), ("195000", "195000"))
        self.assertIn("Anexo N° 221", g["motivo"])                                      # motivo sugerido y editable
        self.assertGreaterEqual(g["recalculos"], 1)
        self.assertIsNone(g["dif"])
        self.assertNotIn("nota", g["clase"])                                            # verde: «Un solo valor»
        self.assertIn("Un solo valor", g["html"])
        s = o["sinFirmar"]["html"]
        self.assertIn("corrige el monto en el anexo", s)                                # sin firmar: se corrige en el anexo
        self.assertNotIn("Igualar la OT al anexo", s)                                   # y sin montos para igualar no hay botón

    def test_el_guardado_ya_no_tiene_confirmacion_ni_aviso_de_anexo(self):
        with open(PLANTILLA, encoding="utf-8") as fh:
            t = fh.read()
        i = t.index("async function otdFinCorrGuardar")
        g = t[i:t.index("</script>", i)]
        self.assertNotIn("aviso_anexo", g)
        self.assertNotIn("_otdFcCostoCambio", g)
        self.assertIn("j.error", g)                                                     # el 409 del servidor se muestra tal cual


if __name__ == "__main__":
    unittest.main()
